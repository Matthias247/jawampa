/*
 * Copyright 2014 Matthias Einwag
 *
 * The jawampa authors license this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package ws.wamp.jawampa;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import rx.Scheduler;
import rx.schedulers.Schedulers;
import ws.wamp.jawampa.WampMessages.*;
import ws.wamp.jawampa.internal.IdGenerator;
import ws.wamp.jawampa.internal.IdValidator;
import ws.wamp.jawampa.internal.RealmConfig;
import ws.wamp.jawampa.internal.UriValidator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@link WampRouter} provides Dealer and Broker functionality for the WAMP
 * protocol.<br>
 */
public class WampRouter {
    
    final static Set<WampRoles> SUPPORTED_CLIENT_ROLES;
    static {
        SUPPORTED_CLIENT_ROLES = new HashSet<WampRoles>();
        SUPPORTED_CLIENT_ROLES.add(WampRoles.Caller);
        SUPPORTED_CLIENT_ROLES.add(WampRoles.Callee);
        SUPPORTED_CLIENT_ROLES.add(WampRoles.Publisher);
        SUPPORTED_CLIENT_ROLES.add(WampRoles.Subscriber);
    }
    
    /** Represents a realm that is exposed through the router */
    static class Realm {
        final RealmConfig config;
        final List<WampRouterHandler> channels = new ArrayList<WampRouterHandler>();
        final Map<String, Procedure> procedures = new HashMap<String, Procedure>();
        
        // Fields that are used for implementing subscription functionality
        final Map<String, Subscription> subscriptionsByTopic = new HashMap<String, Subscription>();
        final Map<Long, Subscription> subscriptionsById = new HashMap<Long, Subscription>();
        long lastUsedSubscriptionId = IdValidator.MIN_VALID_ID;
        
        public Realm(RealmConfig config) {
            this.config = config;
        }
        
        void includeChannel(WampRouterHandler channel, long sessionId, Set<WampRoles> roles) {
            channels.add(channel);
            channel.realm = this;
            channel.sessionId = sessionId;
            channel.roles = roles;
        }
        
        void removeChannel(WampRouterHandler channel, boolean removeFromList) {
            if (channel.realm == null) return;
            
            if (channel.subscriptionsById != null) {
                // Remove the channels subscriptions from our subscription table
                for (Subscription sub : channel.subscriptionsById.values()) {
                    sub.subscribers.remove(channel);
                    if (sub.subscribers.isEmpty()) {
                        // Subscription is no longer used by any client
                        subscriptionsByTopic.remove(sub.topic);
                        subscriptionsById.remove(sub.subscriptionId);
                    }
                }
                channel.subscriptionsById.clear();
                channel.subscriptionsById = null;
            }
            
            if (channel.providedProcedures != null) {
                // Remove the clients procedures from our procedure table
                for (Procedure proc : channel.providedProcedures.values()) {
                    // Clear all pending invocations and thereby inform other clients 
                    // that the proc has gone away
                    for (Invocation invoc : proc.pendingCalls) {
                        if (invoc.caller.state != RouterHandlerState.Open) continue;
                        ErrorMessage errMsg = new ErrorMessage(CallMessage.ID, invoc.callRequestId, 
                            null, ApplicationError.NO_SUCH_PROCEDURE, null, null);
                        invoc.caller.ctx.writeAndFlush(errMsg);
                    }
                    proc.pendingCalls.clear();
                    // Remove the procedure from the realm
                    procedures.remove(proc.procName);
                }
                channel.providedProcedures = null;
                channel.pendingInvocations = null;
            }
            
            channel.realm = null;
            channel.roles.clear();
            channel.roles = null;
            channel.sessionId = 0;
            
            if (removeFromList) {
                channels.remove(channel);
            }
        }
    }
    
    static class Procedure {
        final String procName;
        final WampRouterHandler provider;
        final long registrationId;
        final List<Invocation> pendingCalls = new ArrayList<WampRouter.Invocation>();
        
        public Procedure(String name, WampRouterHandler provider, long registrationId) {
            this.procName = name;
            this.provider = provider;
            this.registrationId = registrationId;
        }
    }
    
    static class Invocation {
        Procedure procedure;
        long callRequestId;
        WampRouterHandler caller;
        long invocationRequestId;
    }
    
    static class Subscription {
        final String topic;
        final long subscriptionId;
        final Set<WampRouterHandler> subscribers;
        
        public Subscription(String topic, long subscriptionId) {
            this.topic = topic;
            this.subscriptionId = subscriptionId;
            this.subscribers = new HashSet<WampRouterHandler>();
        }
    }
    
    final EventLoopGroup eventLoop;
    final Scheduler scheduler;
    
    final ObjectMapper objectMapper = new ObjectMapper();
    
    boolean isDisposed = false;
    
    final Map<String, Realm> realms;
    final ChannelGroup idleChannels;
    
    /**
     * Returns the (singlethreaded) EventLoop on which this router is running.<br>
     * This is required by other Netty ChannelHandlers that want to forward messages
     * to the router.
     */
    public EventLoopGroup eventLoop() {
        return eventLoop;
    }
    
    /**
     * Returns the Jackson {@link ObjectMapper} that is used for JSON serialization,
     * deserialization and object mapping by this router.
     */
    public ObjectMapper objectMapper() {
        return objectMapper;
    }

    WampRouter(Map<String, RealmConfig> realms) {
        
        // Populate the realms from the configuration
        this.realms = new HashMap<String, Realm>();
        for (Map.Entry<String, RealmConfig> e : realms.entrySet()) {
            Realm info = new Realm(e.getValue());
            this.realms.put(e.getKey(), info);
        }
        
        // Create an eventloop and the RX scheduler on top of it
        this.eventLoop = new NioEventLoopGroup(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "WampRouterEventLoop");
                t.setDaemon(true);
                return t;
            }
        });
        this.scheduler = Schedulers.from(eventLoop);
        
        idleChannels = new DefaultChannelGroup(eventLoop.next());
    }
    
    /**
     * Closes the router.<br>
     * This will shut down all realm that are registered to the router.
     * All connections to clients on the realm will be closed.<br>
     * However pending calls will be completed through an error message
     * as far as possible.
     */
    public void close() {
        if (eventLoop.isShuttingDown() || eventLoop.isShutdown()) return;
        
        eventLoop.execute(new Runnable() {
            @Override
            public void run() {
                if (isDisposed) return;
                isDisposed = true;
                
                // Close all currently connected channels
                idleChannels.close();
                idleChannels.clear();
                
                for (Realm ri : realms.values()) {
                    for (WampRouterHandler channel : ri.channels) {
                        ri.removeChannel(channel, false);
                        channel.markAsClosed();
                        GoodbyeMessage goodbye = new GoodbyeMessage(null, ApplicationError.SYSTEM_SHUTDOWN);
                        channel.ctx.writeAndFlush(goodbye).addListener(ChannelFutureListener.CLOSE);
                    }
                    ri.channels.clear();
                }
                
                eventLoop.shutdownGracefully();
            }
        });
    }
    
    public ChannelHandler createRouterHandler() {
        return new WampRouterHandler();
    }
    
    enum RouterHandlerState {
        Open,
        Closed
    }
    
    class WampRouterHandler extends SimpleChannelInboundHandler<WampMessage> {
        
        public RouterHandlerState state = RouterHandlerState.Open;
        ChannelHandlerContext ctx;
        long sessionId;
        Realm realm;
        Set<WampRoles> roles;
        
        /**
         * Procedures that this channel provides.<br>
         * Key is the registration ID, Value is the procedure
         */
        Map<Long, Procedure> providedProcedures;
        
        Map<Long, Invocation> pendingInvocations;
        
        /** The Set of subscriptions to which this channel is subscribed */
        Map<Long, Subscription> subscriptionsById;
        
        long lastUsedId = IdValidator.MIN_VALID_ID;
        
        void markAsClosed() {
            state = RouterHandlerState.Closed;
        }
        
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            // System.out.println("Router handler added on thread " + Thread.currentThread().getId());
            this.ctx = ctx;
        }
        
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            // System.out.println("Router handler active on thread " + Thread.currentThread().getId());
            if (state != RouterHandlerState.Open) return;
            
            if (isDisposed) {
                // Got an incoming connection after the router has already shut down.
                // Therefore we close the connection
                state = RouterHandlerState.Closed;
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            } else {
                idleChannels.add(ctx.channel());
            }
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            // System.out.println("Router handler inactive on thread " + Thread.currentThread().getId());
            if (isDisposed || state != RouterHandlerState.Open) return;
            markAsClosed();
            if (realm != null) {
                realm.removeChannel(this, true);
            } else {
                idleChannels.remove(ctx.channel());
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WampMessage msg) throws Exception {
            //System.out.println("Router channel read on thread " + Thread.currentThread().getId());
            if (isDisposed || state != RouterHandlerState.Open) return;
            if (realm == null) {
                onMessageFromUnregisteredChannel(this, msg);
            } else {
                onMessageFromRegisteredChannel(this, msg);
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (isDisposed || state != RouterHandlerState.Open) return;
            if (realm != null) {
                closeActiveChannel(this, null);  
            } else {
                closePassiveChannel(this);
            }
        }
    }
    
    private void onMessageFromRegisteredChannel(WampRouterHandler handler, WampMessage msg) {
        
        // TODO: Validate roles for all relevant messages
        
        if (msg instanceof HelloMessage || msg instanceof WelcomeMessage) {
            // The client sent hello but it was already registered -> This is an error
            // If the client sends welcome it's also an error
            closeActiveChannel(handler, new GoodbyeMessage(null, ApplicationError.INVALID_ARGUMENT));
        } else if (msg instanceof AbortMessage || msg instanceof GoodbyeMessage) {
            // The client wants to leave the realm
            // Remove the channel from the realm
            handler.realm.removeChannel(handler, true);
            // But add it to the list of passive channels
            idleChannels.add(handler.ctx.channel());
            // Echo the message in case of goodbye
            if (msg instanceof GoodbyeMessage) {
                GoodbyeMessage reply = new GoodbyeMessage(null, ApplicationError.GOODBYE_AND_OUT);
                handler.ctx.writeAndFlush(reply);
            }
        } else if (msg instanceof CallMessage) {
            // The client wants to call a remote function
            // Verify the message
            CallMessage call = (CallMessage) msg;
            String err = null;
            if (!UriValidator.tryValidate(call.procedure, handler.realm.config.useStrictUriValidation)) {
                // Client sent an invalid URI
                err = ApplicationError.INVALID_URI;
            }
            
            if (err == null && !(IdValidator.isValidId(call.requestId))) {
                // Client sent an invalid request ID
                err = ApplicationError.INVALID_ARGUMENT;
            }
            
            Procedure proc = null;
            if (err == null) {
                proc = handler.realm.procedures.get(call.procedure);
                if (proc == null) err = ApplicationError.NO_SUCH_PROCEDURE;
            }
            
            if (err != null) { // If we have an error send that to the client
                ErrorMessage errMsg = new ErrorMessage(CallMessage.ID, call.requestId, 
                    null, err, null, null);
                handler.ctx.writeAndFlush(errMsg);
                return;
            }
            
            // Everything checked, we can forward the call to the provider
            Invocation invoc = new Invocation();
            invoc.callRequestId = call.requestId;
            invoc.caller = handler;
            invoc.procedure = proc;
            invoc.invocationRequestId = IdGenerator.newLinearId(proc.provider.lastUsedId,
                proc.provider.pendingInvocations);
            proc.provider.lastUsedId = invoc.invocationRequestId; 
            
            // Store the invocation
            proc.provider.pendingInvocations.put(invoc.invocationRequestId, invoc);
            // Store the call in the procedure to return error if client unregisters
            proc.pendingCalls.add(invoc);

            // And send it to the provider
            InvocationMessage imsg = new InvocationMessage(invoc.invocationRequestId,
                proc.registrationId, null, call.arguments, call.argumentsKw);
            proc.provider.ctx.writeAndFlush(imsg);
        } else if (msg instanceof YieldMessage) {
            // The clients sends as the result of an RPC
            // Verify the message
            YieldMessage yield = (YieldMessage) msg;
            if (!(IdValidator.isValidId(yield.requestId))) return;
            // Look up the invocation to find the original caller
            if (handler.pendingInvocations == null) return;  // If a client send a yield without an invocation, return
            Invocation invoc = handler.pendingInvocations.get(yield.requestId);
            if (invoc == null) return; // There is no invocation pending under this ID
            handler.pendingInvocations.remove(yield.requestId);
            invoc.procedure.pendingCalls.remove(invoc);
            // Send the result to the original caller
            ResultMessage result = new ResultMessage(invoc.callRequestId, null, yield.arguments, yield.argumentsKw);
            invoc.caller.ctx.writeAndFlush(result);
        } else if (msg instanceof ErrorMessage) {
            ErrorMessage err = (ErrorMessage) msg;
            if (!(IdValidator.isValidId(err.requestId))) {
                return;
            }
            if (err.requestType == InvocationMessage.ID) {
                if (!UriValidator.tryValidate(err.error, handler.realm.config.useStrictUriValidation)) {
                    // The Message provider has sent us an invalid URI for the error string
                    // We better don't forward it but instead close the connection, which will
                    // give the original caller an unknown message error
                    closeActiveChannel(handler, new GoodbyeMessage(null, ApplicationError.INVALID_ARGUMENT));
                    return;
                }
                
                // Look up the invocation to find the original caller
                if (handler.pendingInvocations == null) return; // if an error is send before an invocation, do not do anything
                Invocation invoc = handler.pendingInvocations.get(err.requestId);
                if (invoc == null) return; // There is no invocation pending under this ID
                handler.pendingInvocations.remove(err.requestId);
                invoc.procedure.pendingCalls.remove(invoc);
                
                // Send the result to the original caller
                ErrorMessage fwdError = new ErrorMessage(CallMessage.ID, invoc.callRequestId, 
                    null, err.error, err.arguments, err.argumentsKw);
                invoc.caller.ctx.writeAndFlush(fwdError);                
            }
            // else TODO: Are there any other possibilities where a client could return ERROR
        } else if (msg instanceof RegisterMessage) {
            // The client wants to register a procedure
            // Verify the message
            RegisterMessage reg = (RegisterMessage) msg;
            String err = null;
            if (!UriValidator.tryValidate(reg.procedure, handler.realm.config.useStrictUriValidation)) {
                // Client sent an invalid URI
                err = ApplicationError.INVALID_URI;
            }
            
            if (err == null && !(IdValidator.isValidId(reg.requestId))) {
                // Client sent an invalid request ID
                err = ApplicationError.INVALID_ARGUMENT;
            }
            
            Procedure proc = null;
            if (err == null) {
                proc = handler.realm.procedures.get(reg.procedure);
                if (proc != null) err = ApplicationError.PROCEDURE_ALREADY_EXISTS;
            }
            
            if (err != null) { // If we have an error send that to the client
                ErrorMessage errMsg = new ErrorMessage(RegisterMessage.ID, reg.requestId, 
                    null, err, null, null);
                handler.ctx.writeAndFlush(errMsg);
                return;
            }
            
            // Everything checked, we can register the caller as the procedure provider
            long registrationId = IdGenerator.newLinearId(handler.lastUsedId, handler.providedProcedures);
            handler.lastUsedId = registrationId;
            Procedure procInfo = new Procedure(reg.procedure, handler, registrationId);
            
            // Insert new procedure
            handler.realm.procedures.put(reg.procedure, procInfo);
            if (handler.providedProcedures == null) {
                handler.providedProcedures = new HashMap<Long, WampRouter.Procedure>();
                handler.pendingInvocations = new HashMap<Long, WampRouter.Invocation>();
            }
            handler.providedProcedures.put(procInfo.registrationId, procInfo);
            
            RegisteredMessage response = new RegisteredMessage(reg.requestId, procInfo.registrationId);
            handler.ctx.writeAndFlush(response);
        } else if (msg instanceof UnregisterMessage) {
            // The client wants to unregister a procedure
            // Verify the message
            UnregisterMessage unreg = (UnregisterMessage) msg;
            String err = null;
            if (!(IdValidator.isValidId(unreg.requestId))
             || !(IdValidator.isValidId(unreg.registrationId))) {
                // Client sent an invalid request or registration ID
                err = ApplicationError.INVALID_ARGUMENT;
            }
            
            Procedure proc = null;
            if (err == null) {
                if (handler.providedProcedures != null) {
                    proc = handler.providedProcedures.get(unreg.registrationId);
                }
                // Check whether the procedure exists AND if the caller is the owner
                // If the caller is not the owner it might be an attack, so we don't
                // disclose that the procedure exists.
                if (proc == null) {
                    err = ApplicationError.NO_SUCH_REGISTRATION;
                }
            }
            
            if (err != null) { // If we have an error send that to the client
                ErrorMessage errMsg = new ErrorMessage(UnregisterMessage.ID, unreg.requestId, 
                    null, err, null, null);
                handler.ctx.writeAndFlush(errMsg);
                return;
            }
            
            // Mark pending calls to this procedure as failed
            for (Invocation invoc : proc.pendingCalls) {
                handler.pendingInvocations.remove(invoc.invocationRequestId);
                if (invoc.caller.state == RouterHandlerState.Open) {
                    ErrorMessage errMsg = new ErrorMessage(CallMessage.ID, invoc.callRequestId, 
                        null, ApplicationError.NO_SUCH_PROCEDURE, null, null);
                    invoc.caller.ctx.writeAndFlush(errMsg);
                }
            }
            proc.pendingCalls.clear();

            // Remove the procedure from the realm and the handler
            handler.realm.procedures.remove(proc.procName);
            handler.providedProcedures.remove(proc.registrationId);
            
            if (handler.providedProcedures.size() == 0) {
                handler.providedProcedures = null;
                handler.pendingInvocations = null;
            }
            
            // Send the acknowledge
            UnregisteredMessage response = new UnregisteredMessage(unreg.requestId);
            handler.ctx.writeAndFlush(response);
        } else if (msg instanceof SubscribeMessage) {
            // The client wants to subscribe to a procedure
            // Verify the message
            SubscribeMessage sub = (SubscribeMessage) msg;
            String err = null;
            if (!UriValidator.tryValidate(sub.topic, handler.realm.config.useStrictUriValidation)) {
                // Client sent an invalid URI
                err = ApplicationError.INVALID_URI;
            }
            
            if (err == null && !(IdValidator.isValidId(sub.requestId))) {
                // Client sent an invalid request ID
                err = ApplicationError.INVALID_ARGUMENT;
            }
            
            if (err != null) { // If we have an error send that to the client
                ErrorMessage errMsg = new ErrorMessage(SubscribeMessage.ID, sub.requestId, 
                    null, err, null, null);
                handler.ctx.writeAndFlush(errMsg);
                return;
            }
            
            // Create a new subscription map for the client if it was not subscribed before
            if (handler.subscriptionsById == null) {
                handler.subscriptionsById = new HashMap<Long, WampRouter.Subscription>();
            }
            
            // Search if a subscription from any client on the realm to this topic exists
            Subscription subscription = handler.realm.subscriptionsByTopic.get(sub.topic);
            if (subscription == null) {
                // No client was subscribed to this URI up to now
                // Create a new subscription id
                long subscriptionId = IdGenerator.newLinearId(handler.realm.lastUsedSubscriptionId,
                                                              handler.realm.subscriptionsById);
                handler.realm.lastUsedSubscriptionId = subscriptionId;
                // Create and add the new subscription
                subscription = new Subscription(sub.topic, subscriptionId);
                handler.realm.subscriptionsByTopic.put(sub.topic, subscription);
                handler.realm.subscriptionsById.put(subscriptionId, subscription);
            }
            
            // We check if the client is already subscribed to this topic by trying to add the
            // new client as a receiver. If the client is already a receiver we do nothing
            // (already subscribed and already stored in handler.subscriptionsById). Calling
            // add to check and add is more efficient than checking with contains first.
            // If the client was already subscribed this will return the same subscriptionId
            // than as for the last subscription.
            // See discussion in https://groups.google.com/forum/#!topic/wampws/kC878Ngc9Z0
            if (subscription.subscribers.add(handler)) {
                // Add the subscription on the client
                handler.subscriptionsById.put(subscription.subscriptionId, subscription);
            }
            
            SubscribedMessage response = new SubscribedMessage(sub.requestId, subscription.subscriptionId);
            handler.ctx.writeAndFlush(response);
        } else if (msg instanceof UnsubscribeMessage) {
            // The client wants to cancel a subscription
            // Verify the message
            UnsubscribeMessage unsub = (UnsubscribeMessage) msg;
            String err = null;
            if (!(IdValidator.isValidId(unsub.requestId))
             || !(IdValidator.isValidId(unsub.subscriptionId))) {
                // Client sent an invalid request or registration ID
                err = ApplicationError.INVALID_ARGUMENT;
            }
            
            Subscription s = null;
            if (err == null) {
                // Check whether such a subscription exists and fetch the topic name
                if (handler.subscriptionsById != null) {
                    s = handler.subscriptionsById.get(unsub.subscriptionId);
                }
                if (s == null) {
                    err = ApplicationError.NO_SUCH_SUBSCRIPTION;
                }
            }
            
            if (err != null) { // If we have an error send that to the client
                ErrorMessage errMsg = new ErrorMessage(UnsubscribeMessage.ID, unsub.requestId, 
                    null, err, null, null);
                handler.ctx.writeAndFlush(errMsg);
                return;
            }

            // Remove the channel as an receiver from the subscription
            s.subscribers.remove(handler);
            
            // Remove the subscription from the handler
            handler.subscriptionsById.remove(s.subscriptionId);
            if (handler.subscriptionsById.isEmpty()) {
                handler.subscriptionsById = null;
            }
            
            // Remove the subscription from the realm if no subscriber is left
            if (s.subscribers.isEmpty()) {
                handler.realm.subscriptionsByTopic.remove(s.topic);
                handler.realm.subscriptionsById.remove(s.subscriptionId);
            }
            
            // Send the acknowledge
            UnsubscribedMessage response = new UnsubscribedMessage(unsub.requestId);
            handler.ctx.writeAndFlush(response);
        } else if (msg instanceof PublishMessage) {
            // The client wants to publish something to all subscribers (apart from himself)
            PublishMessage pub = (PublishMessage) msg;
            // Check whether the client wants an acknowledgement for the publication
            // Default is no
            boolean sendAcknowledge = false;
            JsonNode ackOption = pub.options.get("acknowledge");
            if (ackOption != null && ackOption.asBoolean() == true)
                sendAcknowledge = true;
            
            String err = null;
            if (!UriValidator.tryValidate(pub.topic, handler.realm.config.useStrictUriValidation)) {
                // Client sent an invalid URI
                err = ApplicationError.INVALID_URI;
            }
            
            if (err == null && !(IdValidator.isValidId(pub.requestId))) {
                // Client sent an invalid request ID
                err = ApplicationError.INVALID_ARGUMENT;
            }
            
            if (err != null) { // If we have an error send that to the client
                ErrorMessage errMsg = new ErrorMessage(PublishMessage.ID, pub.requestId, 
                    null, err, null, null);
                if (sendAcknowledge) {
                    handler.ctx.writeAndFlush(errMsg);
                }
                return;
            }
            
            long publicationId = IdGenerator.newRandomId(null); // Store that somewhere?

            // Get the subscriptions for this topic on the realm
            Subscription subscription = handler.realm.subscriptionsByTopic.get(pub.topic);
            if (subscription != null) {
                for (WampRouterHandler receiver : subscription.subscribers) {
                    if (receiver == handler) { // Potentially skip the publisher
                        boolean skipPublisher = true;
                        if (pub.options != null) {
                            JsonNode excludeMeNode = pub.options.get("exclude_me");
                            if (excludeMeNode != null) {
                                skipPublisher = excludeMeNode.asBoolean(true);
                            }
                        }
                        if (skipPublisher) continue;
                    }
                    // Publish the event to the subscriber
                    EventMessage ev = new EventMessage(subscription.subscriptionId, publicationId,
                        null, pub.arguments, pub.argumentsKw);
                    receiver.ctx.writeAndFlush(ev);
                }
            }
            
            if (sendAcknowledge) {
                PublishedMessage response = new PublishedMessage(pub.requestId, publicationId);
                handler.ctx.writeAndFlush(response);
            }
        }
    }
    
    private void onMessageFromUnregisteredChannel(WampRouterHandler channelHandler, WampMessage msg)
    {
        // Only HELLO is allowed when a channel is not registered
        if (!(msg instanceof HelloMessage)) {
            // Close the connection
            closePassiveChannel(channelHandler);
            return;
        }
        
        HelloMessage hello = (HelloMessage) msg;
        
        String errorMsg = null;
        Realm realm = null;
        if (!UriValidator.tryValidate(hello.realm, false)) {
            errorMsg = ApplicationError.INVALID_URI;
        } else {
            realm = realms.get(hello.realm);
            if (realm == null) {
                errorMsg = ApplicationError.NO_SUCH_REALM;
            }
        }
        
        if (errorMsg != null) {
            AbortMessage abort = new AbortMessage(null, errorMsg);
            channelHandler.ctx.writeAndFlush(abort);
            return;
        }
        
        Set<WampRoles> roles = new HashSet<WampRoles>();
        boolean hasUnsupportedRoles = false;
        
        JsonNode n = hello.details.get("roles");
        if (n != null && n.isObject()) {
            ObjectNode rolesNode = (ObjectNode) n;
            Iterator<String> roleKeys = rolesNode.fieldNames();
            while (roleKeys.hasNext()) {
                WampRoles role = WampRoles.fromString(roleKeys.next());
                if (!SUPPORTED_CLIENT_ROLES.contains(role)) hasUnsupportedRoles = true;
                if (role != null) roles.add(role);
            }
        }
        
        if (roles.size() == 0 || hasUnsupportedRoles) {
            AbortMessage abort = new AbortMessage(null, ApplicationError.NO_SUCH_ROLE);
            channelHandler.ctx.writeAndFlush(abort);
            return;
        }
        
        long sessionId = IdGenerator.newRandomId(null);
        // TODO: Should be unique on the router and should be stored somewhere
        
        // Include the channel into the realm
        realm.includeChannel(channelHandler, sessionId, roles);
        // Remove the channel from the idle channel list - It is no longer idle
        idleChannels.remove(channelHandler.ctx.channel());
        
        // Expose the roles that are configured for the realm
        ObjectNode welcomeDetails = objectMapper.createObjectNode();
        ObjectNode routerRoles = welcomeDetails.putObject("roles");
        for (WampRoles role : realm.config.roles) {
            ObjectNode roleNode = routerRoles.putObject(role.toString());
            if (role == WampRoles.Publisher ) {
                ObjectNode featuresNode = roleNode.putObject("features");
                featuresNode.put("publisher_exclusion", true);
            }
        }
        
        // Respond with the WELCOME message
        WelcomeMessage welcome = new WelcomeMessage(channelHandler.sessionId, welcomeDetails);
        channelHandler.ctx.writeAndFlush(welcome);
    }
    
    private void closeActiveChannel(WampRouterHandler channel, WampMessage closeMessage) {
        if (channel == null) return;
        
        channel.realm.removeChannel(channel, true);
        channel.markAsClosed();
        
        if (channel.ctx != null) {
            Object m = (closeMessage == null) ? Unpooled.EMPTY_BUFFER : closeMessage;
            channel.ctx.writeAndFlush(m)
               .addListener(ChannelFutureListener.CLOSE);
        }
    }
    
    private void closePassiveChannel(WampRouterHandler channelHandler) {
        idleChannels.remove(channelHandler.ctx.channel());
        channelHandler.markAsClosed();
        channelHandler.ctx.close();
    }
}
