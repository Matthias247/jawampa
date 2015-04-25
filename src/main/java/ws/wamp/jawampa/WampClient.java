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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.BehaviorSubject;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.Subscriptions;
import ws.wamp.jawampa.WampMessages.AbortMessage;
import ws.wamp.jawampa.WampMessages.CallMessage;
import ws.wamp.jawampa.WampMessages.ErrorMessage;
import ws.wamp.jawampa.WampMessages.EventMessage;
import ws.wamp.jawampa.WampMessages.GoodbyeMessage;
import ws.wamp.jawampa.WampMessages.InvocationMessage;
import ws.wamp.jawampa.WampMessages.PublishedMessage;
import ws.wamp.jawampa.WampMessages.RegisterMessage;
import ws.wamp.jawampa.WampMessages.RegisteredMessage;
import ws.wamp.jawampa.WampMessages.ResultMessage;
import ws.wamp.jawampa.WampMessages.SubscribeMessage;
import ws.wamp.jawampa.WampMessages.SubscribedMessage;
import ws.wamp.jawampa.WampMessages.UnregisterMessage;
import ws.wamp.jawampa.WampMessages.UnregisteredMessage;
import ws.wamp.jawampa.WampMessages.UnsubscribeMessage;
import ws.wamp.jawampa.WampMessages.UnsubscribedMessage;
import ws.wamp.jawampa.WampMessages.WampMessage;
import ws.wamp.jawampa.WampMessages.WelcomeMessage;
import ws.wamp.jawampa.internal.IdGenerator;
import ws.wamp.jawampa.internal.IdValidator;
import ws.wamp.jawampa.internal.Promise;
import ws.wamp.jawampa.internal.UriValidator;
import ws.wamp.jawampa.transport.WampChannelEvents;
import ws.wamp.jawampa.transport.WampClientChannelFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Provides the client-side functionality for WAMP.<br>
 * The {@link WampClient} allows to make remote procedudure calls, subscribe to
 * and publish events and to register functions for RPC.<br>
 * It has to be constructed through a {@link WampClientBuilder} and can not
 * directly be instantiated.
 */
public class WampClient {
    
    /**
     * Possible states for a WAMP session between client and router
     */
    public static enum Status {
        /** The session is not connected */
        Disconnected,
        /** The session is trying to connect to the router */
        Connecting,
        /** The session is connected to the router */
        Connected
    }

    /** The current status */
    Status status = Status.Disconnected;
    BehaviorSubject<Status> statusObservable = BehaviorSubject.create(Status.Disconnected);
    
    final EventLoopGroup eventLoop;
    final Scheduler scheduler;
    
    final ObjectMapper objectMapper = new ObjectMapper();

    final URI routerUri;
    final String realm;
    final boolean useStrictUriValidation;
    
    /** Returns the URI of the router to which this client is connected */
    public URI routerUri() {
        return routerUri;
    }

    /** Returns the name of the realm on the router */
    public String realm() {
        return realm;
    }
    
    final boolean closeClientOnErrors;
    boolean isCompleted = false;

    /** The factory which is used to create new transports to the remote peer */
    final WampClientChannelFactory channelFactory;
    Channel channel;
    ChannelFuture connectFuture;
    SessionHandler handler;
    
    long lastRequestId = IdValidator.MIN_VALID_ID;
    
    final int totalNrReconnects;
    final int reconnectInterval;
    int remainingNrReconnects = 0;
    Subscription reconnectSubscription;
    
    long sessionId;
    ObjectNode welcomeDetails = null;
    final WampRoles[] clientRoles;
    WampRoles[] routerRoles;
    
    enum PubSubState {
        Subscribing,
        Subscribed,
        Unsubscribing,
        Unsubscribed
    }
    
    enum RegistrationState {
        Registering,
        Registered,
        Unregistering,
        Unregistered
    }
    
    static class RequestMapEntry {
        public final int requestType;
        public final AsyncSubject<?> resultSubject;
        
        public RequestMapEntry(int requestType, AsyncSubject<?> resultSubject) {
            this.requestType = requestType;
            this.resultSubject = resultSubject;
        }
    }
    
    static class SubscriptionMapEntry {
        public PubSubState state;
        public long subscriptionId = 0;
        
        public final List<Subscriber<? super PubSubData>> subscribers
            = new ArrayList<Subscriber<? super PubSubData>>();
        
        public SubscriptionMapEntry(PubSubState state) {
            this.state = state;
        }
    }
    
    static class RegisteredProceduresMapEntry {
        public RegistrationState state;
        public long registrationId = 0;
        public final Subscriber<? super Request> subscriber;
        
        public RegisteredProceduresMapEntry(Subscriber<? super Request> subscriber, RegistrationState state) {
            this.subscriber = subscriber;
            this.state = state;
        }
    }
    
    HashMap<Long, RequestMapEntry> requestMap = 
        new HashMap<Long, WampClient.RequestMapEntry>();
    
    HashMap<String, SubscriptionMapEntry> subscriptionsByUri =
        new HashMap<String, SubscriptionMapEntry>();
    HashMap<Long, SubscriptionMapEntry> subscriptionsBySubscriptionId =
        new HashMap<Long, SubscriptionMapEntry>();
    
    HashMap<String, RegisteredProceduresMapEntry> registeredProceduresByUri = 
            new HashMap<String, RegisteredProceduresMapEntry>();
    HashMap<Long, RegisteredProceduresMapEntry> registeredProceduresById = 
            new HashMap<Long, RegisteredProceduresMapEntry>();
    
    
    WampClient(URI routerUri, String realm, WampRoles[] roles,
               boolean useStrictUriValidation,
               boolean closeClientOnErrors,
               WampClientChannelFactory channelFactory,
               int nrReconnects, int reconnectInterval)
    {
        // Create an eventloop and the RX scheduler on top of it
        this.eventLoop = new NioEventLoopGroup(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "WampClientEventLoop");
                t.setDaemon(true);
                return t;
            }
        });
        this.scheduler = Schedulers.from(eventLoop);
        
        this.routerUri = routerUri;
        this.realm = realm;
        this.clientRoles = roles;
        this.useStrictUriValidation = useStrictUriValidation;
        this.closeClientOnErrors = closeClientOnErrors;
        this.channelFactory = channelFactory;
        this.totalNrReconnects = nrReconnects;
        this.reconnectInterval = reconnectInterval;
    }

    private void completeStatus(Exception e) {
        if (isCompleted) return;
        isCompleted = true;

        if (e != null)
            statusObservable.onError(e);
        else
            statusObservable.onCompleted();
    }

    /**
     * Opens the session<br>
     * This should be called after a subscription on {@link #statusChanged}
     * was installed.<br>
     * If the session was already opened this has no effect besides
     * resetting the reconnect counter.<br>
     * If the session was already closed through a call to {@link #close}
     * no new connect attempt will be performed.
     */
    public void open() {
        eventLoop.execute(new Runnable() {
            @Override
            public void run() {
                if (isCompleted) return;
                // Reset the number of reconnects
                // This happens in both connecting and disconnected case
                remainingNrReconnects = totalNrReconnects;
                if (remainingNrReconnects > 0) remainingNrReconnects--;
                if (status == Status.Disconnected) {
                    status = Status.Connecting;
                    statusObservable.onNext(status);
                    beginConnect();
                }
            }
        });
    }
    
    /**
     * Returns whether reconnects are allowed (true) or not (false)
     */
    private boolean mayReconnect() {
        return remainingNrReconnects != 0;
    }
    
    /** 
     * Schedules a reconnect operation<br>
     * The reconnect can be canceled by unsubscribing the {@link #reconnectSubscription} 
     * */
    private void scheduleReconnect() {
        // Check for possible reconnects
        if (remainingNrReconnects == 0) return;
        
        status = Status.Connecting;
        
        // Decrease remaining number of reconnects if it's not infinite
        if (remainingNrReconnects > 0) remainingNrReconnects--;
        // Make a composite subscription that is used to cancel the
        // reconnect. The status of it can be checked inside the callback
        final CompositeSubscription sub = new CompositeSubscription();
        sub.add(scheduler.createWorker().schedule(new Action0() {
            @Override
            public void call() {
                if (reconnectSubscription.isUnsubscribed()) return; 
                beginConnect();
            }
        }, reconnectInterval, TimeUnit.MILLISECONDS));
        reconnectSubscription = sub; // Store it as our new reconnect subscription
    }
    
    /**
     * Netty handler for that receives and processes WampMessages and state
     * events from the pipeline.
     * A new instance of this is created for each connection attempt.
     */
    private class SessionHandler extends SimpleChannelInboundHandler<WampMessage> {
        
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            // System.out.println("Session handler added");
        }
        
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            if (handler != this) return;
         // System.out.println("Session handler active");
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (handler != this) return;
            // System.out.println("Session handler inactive");
            
            closeCurrentTransport();
            // Can emit the status, because we always go from connected to closed here
            statusObservable.onNext(status);
            // Try reconnect if possible
            if (mayReconnect()) {
                scheduleReconnect();
                statusObservable.onNext(status);
            }
        }
        
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (handler != this) return;
            
            if (evt == WampChannelEvents.WEBSOCKET_CONN_ESTABLISHED) {
                // System.out.println("Session websocket connection established");
                // Connection to the remote host was established
                // However the WAMP session is not established until the handshake was finished
                
                // Put the requested roles in the Hello message
                ObjectNode o = objectMapper.createObjectNode();
                ObjectNode rolesNode = o.putObject("roles");
                for (WampRoles role : clientRoles) {
                    ObjectNode roleNode = rolesNode.putObject(role.toString());
                    if (role == WampRoles.Publisher ) {
                        ObjectNode featuresNode = roleNode.putObject("features");
                        featuresNode.put("publisher_exclusion", true);
                    }
                }
                
                ctx.writeAndFlush(new WampMessages.HelloMessage(realm, o));
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WampMessage msg) throws Exception {
            if (handler != this) return;
            onMessageReceived(msg);
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            // System.out.println("Session handler caught exception " + cause);
            super.exceptionCaught(ctx, cause);
        }
        
        
    }

    /**
     * Starts an connection attempt to the router
     */
    private void beginConnect() {
        handler = new SessionHandler();
        try {
            connectFuture = channelFactory.createChannel(handler, eventLoop);
            connectFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        if (f == connectFuture) {
                            // Our new channel is connected
                            channel = f.channel();
                            connectFuture = null;
                        } else {
                            // We we're connected but aren't interested in the channel anymore
                            // Therefore we close the new channel
                            f.channel().writeAndFlush(Unpooled.EMPTY_BUFFER)
                                       .addListener(ChannelFutureListener.CLOSE);
                        }
                    } else if (!f.isCancelled()) {
                        // Remark: Might be called directly in addListener
                        // Therefore addListener should be the last call in beginConnect
                        closeCurrentTransport();
                        // Try reconnect if possible, otherwise announce close
                        if (mayReconnect()) {
                            scheduleReconnect();
                        } else {
                            statusObservable.onNext(status);
                        }
                    }
                }
            });
        } catch (Exception e) {
            // Catch exceptions that can happen during creating the channel
            // These are normally signs that something is wrong with our configuration
            // Therefore we don't trigger retries
            closeCurrentTransport();
            statusObservable.onNext(status);
            completeStatus(e);
        }
    }

    private void closeCurrentTransport() {
        if (status == Status.Disconnected) return;
        
        if (channel != null) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
                   .addListener(ChannelFutureListener.CLOSE);
            channel = null;
        }
        
        // If we are in the connect process mark the connect future as cancelled
        if (connectFuture != null) connectFuture.cancel(false);
        connectFuture = null;
        handler = null;
        
        // Stop the reconnect timer. In case of real reconnects
        // it will be initialized after closeCurrentTransport()
        if (reconnectSubscription != null) {
            reconnectSubscription.unsubscribe();
            reconnectSubscription = null;
        }

        welcomeDetails = null;
        sessionId = 0;

        status = Status.Disconnected;
        
        clearPendingRequests(new ApplicationError(ApplicationError.TRANSPORT_CLOSED));
        clearAllSubscriptions(null);
        clearAllRegisteredProcedures(null);
    }

    /**
     * Closes the session.<br>
     * It will not be possible to open the session again with {@link #open} for safety
     * reasons. If a new session is required a new {@link WampClient} should be built
     * through the used {@link WampClientBuilder}.
     */
    public void close() {
        
        // Avoid crashes on multiple/concurrent shutdowns
        if (eventLoop.isShuttingDown() || eventLoop.isShutdown()) return;
        
        eventLoop.execute(new Runnable() {
            @Override
            public void run() {
                if (!isCompleted) // Check if already closed
                {
                    if (status == Status.Connected) {
                        // Send goodbye to the remote
                        GoodbyeMessage msg = new GoodbyeMessage(null, 
                            ApplicationError.SYSTEM_SHUTDOWN);
                        channel.writeAndFlush(msg);
                    }
                    
                    if (status != Status.Disconnected) {
                        // Close the connection (or connection attempt)
                        remainingNrReconnects = 0; // Don't reconnect
                        closeCurrentTransport();
                        statusObservable.onNext(status);
                    }
                    
                    // Normal close without an error
                    completeStatus(null);
                }
                
                // Shut down the eventLoop if it didn't happen before
                if (eventLoop.isShuttingDown() || eventLoop.isShutdown()) return;
                eventLoop.shutdownGracefully();
            }
        });
    }

    /**
     * An Observable that allows to monitor the connection status of the Session.
     */
    public Observable<Status> statusChanged() {
        return statusObservable;
    }
    
    private void onProtocolError() {
        onSessionError(new ApplicationError(ApplicationError.PROTCOL_ERROR));
    }
    
    private void onSessionError(ApplicationError error) {
        // We move from connected to disconnected
        closeCurrentTransport();
        statusObservable.onNext(status);
        
        if (closeClientOnErrors) {
            remainingNrReconnects = 0;
            completeStatus(error);
        }
        else {
            if (mayReconnect()) {
                scheduleReconnect();
                statusObservable.onNext(status);
            }
        }
    }

    private void onMessageReceived(WampMessage msg) {
        if (welcomeDetails == null) {
            // We were not yet welcomed
            if (msg instanceof WelcomeMessage) {
                // Receive a welcome. Now the session is established!
                welcomeDetails = ((WelcomeMessage) msg).details;
                sessionId = ((WelcomeMessage) msg).sessionId;
                
                // Extract the roles of the remote side
                JsonNode roleNode = welcomeDetails.get("roles");
                if (roleNode == null || !roleNode.isObject()) {
                    onProtocolError();
                    return;
                }
                
                routerRoles = null;
                Set<WampRoles> rroles = new HashSet<WampRoles>();
                Iterator<String> roleKeys = roleNode.fieldNames();
                while (roleKeys.hasNext()) {
                    WampRoles role = WampRoles.fromString(roleKeys.next());
                    if (role != null) rroles.add(role);
                }
                routerRoles = new WampRoles[rroles.size()];
                int i = 0;
                for (WampRoles r : rroles) {
                    routerRoles[i] = r; 
                    i++;
                }
                
                remainingNrReconnects = totalNrReconnects;
                status = Status.Connected;
                statusObservable.onNext(status);
            }
            else if (msg instanceof AbortMessage) {
                // The remote doesn't want us to connect :(
                AbortMessage abort = (AbortMessage) msg;
                onSessionError(new ApplicationError(abort.reason));
            }
        }
        else {
            // We were already welcomed
            if (msg instanceof WelcomeMessage) {
                onProtocolError();
            }
            else if (msg instanceof AbortMessage) {
                onProtocolError();
            }
            else if (msg instanceof GoodbyeMessage) {
                // Reply the goodbye
                channel.writeAndFlush(new GoodbyeMessage(null, ApplicationError.GOODBYE_AND_OUT));
                // We could also use the reason from the msg, but this would be harder
                // to determinate from a "real" error
                onSessionError(new ApplicationError(ApplicationError.GOODBYE_AND_OUT));
            }
            else if (msg instanceof ResultMessage) {
                ResultMessage r = (ResultMessage)msg;
                RequestMapEntry requestInfo = requestMap.get(r.requestId);
                if (requestInfo == null) return; // Ignore the result
                if (requestInfo.requestType != WampMessages.CallMessage.ID) {
                    onProtocolError();
                    return;
                }
                requestMap.remove(r.requestId);
                Reply reply = new Reply(r.arguments, r.argumentsKw);
                @SuppressWarnings("unchecked")
                AsyncSubject<Reply> subject = (AsyncSubject<Reply>)requestInfo.resultSubject;
                subject.onNext(reply);
                subject.onCompleted();
            }
            else if (msg instanceof ErrorMessage) {
                ErrorMessage r = (ErrorMessage)msg;
                if (r.requestType == WampMessages.CallMessage.ID
                 || r.requestType == WampMessages.SubscribeMessage.ID
                 || r.requestType == WampMessages.UnsubscribeMessage.ID
                 || r.requestType == WampMessages.PublishMessage.ID
                 || r.requestType == WampMessages.RegisterMessage.ID
                 || r.requestType == WampMessages.UnregisterMessage.ID)
                {
                    RequestMapEntry requestInfo = requestMap.get(r.requestId);
                    if (requestInfo == null) return; // Ignore the error
                    // Check whether the request type we sent equals the
                    // request type for the error we receive
                    if (requestInfo.requestType != r.requestType) {
                        onProtocolError();
                        return;
                    }
                    requestMap.remove(r.requestId);
                    ApplicationError err = new ApplicationError(r.error, r.arguments, r.argumentsKw);
                    requestInfo.resultSubject.onError(err);
                }
            }
            else if (msg instanceof SubscribedMessage) {
                SubscribedMessage m = (SubscribedMessage)msg;
                RequestMapEntry requestInfo = requestMap.get(m.requestId);
                if (requestInfo == null) return; // Ignore the result
                if (requestInfo.requestType != WampMessages.SubscribeMessage.ID) {
                    onProtocolError();
                    return;
                }
                requestMap.remove(m.requestId);
                @SuppressWarnings("unchecked")
                AsyncSubject<Long> subject = (AsyncSubject<Long>)requestInfo.resultSubject;
                subject.onNext(m.subscriptionId);
                subject.onCompleted();
            }
            else if (msg instanceof UnsubscribedMessage) {
            	UnsubscribedMessage m = (UnsubscribedMessage)msg;
                RequestMapEntry requestInfo = requestMap.get(m.requestId);
                if (requestInfo == null) return; // Ignore the result
                if (requestInfo.requestType != WampMessages.UnsubscribeMessage.ID) {
                    onProtocolError();
                    return;
                }
                requestMap.remove(m.requestId);
                @SuppressWarnings("unchecked")
                AsyncSubject<Void> subject = (AsyncSubject<Void>)requestInfo.resultSubject;
                subject.onNext(null);
                subject.onCompleted();
            }
            else if (msg instanceof EventMessage) {
                EventMessage ev = (EventMessage)msg;
                SubscriptionMapEntry entry = subscriptionsBySubscriptionId.get(ev.subscriptionId);
                if (entry == null || entry.state != PubSubState.Subscribed) return; // Ignore the result
                PubSubData evResult = new PubSubData(ev.arguments, ev.argumentsKw);
                // publish the event
                for (Subscriber<? super PubSubData> s : entry.subscribers) {
                    s.onNext(evResult);
                }
            }
            else if (msg instanceof PublishedMessage) {
                PublishedMessage m = (PublishedMessage)msg;
                RequestMapEntry requestInfo = requestMap.get(m.requestId);
                if (requestInfo == null) return; // Ignore the result
                if (requestInfo.requestType != WampMessages.PublishMessage.ID) {
                    onProtocolError();
                    return;
                }
                requestMap.remove(m.requestId);
                @SuppressWarnings("unchecked")
                AsyncSubject<Long> subject = (AsyncSubject<Long>)requestInfo.resultSubject;
                subject.onNext(m.publicationId);
                subject.onCompleted();
            }
            else if (msg instanceof RegisteredMessage) {
                RegisteredMessage m = (RegisteredMessage)msg;
                RequestMapEntry requestInfo = requestMap.get(m.requestId);
                if (requestInfo == null) return; // Ignore the result
                if (requestInfo.requestType != WampMessages.RegisterMessage.ID) {
                    onProtocolError();
                    return;
                }
                requestMap.remove(m.requestId);
                @SuppressWarnings("unchecked")
                AsyncSubject<Long> subject = (AsyncSubject<Long>)requestInfo.resultSubject;
                subject.onNext(m.registrationId);
                subject.onCompleted();
            }
            else if (msg instanceof UnregisteredMessage) {
                UnregisteredMessage m = (UnregisteredMessage)msg;
                RequestMapEntry requestInfo = requestMap.get(m.requestId);
                if (requestInfo == null) return; // Ignore the result
                if (requestInfo.requestType != WampMessages.UnregisterMessage.ID) {
                    onProtocolError();
                    return;
                }
                requestMap.remove(m.requestId);
                @SuppressWarnings("unchecked")
                AsyncSubject<Void> subject = (AsyncSubject<Void>)requestInfo.resultSubject;
                subject.onNext(null);
                subject.onCompleted();
            }
            else if (msg instanceof InvocationMessage) {
                InvocationMessage m = (InvocationMessage)msg;
                RegisteredProceduresMapEntry entry = registeredProceduresById.get(m.registrationId);
                if (entry == null || entry.state != RegistrationState.Registered) {
                    // Send an error that we are no longer registered
                    channel.writeAndFlush(new ErrorMessage(InvocationMessage.ID, m.requestId, null,
                                          ApplicationError.NO_SUCH_PROCEDURE, null, null));
                }
                else {
                    // Send the request to the subscriber, which can then send responses
                    Request request = new Request(this, channel, m.requestId, m.arguments, m.argumentsKw);
                    entry.subscriber.onNext(request);
                }
            }
            else {
                // Unknown message
            }
        }
    }
    
    /**
     * Builds an ArrayNode from all positional arguments in a WAMP message.<br>
     * If there are no positional arguments then null will be returned, as
     * WAMP requires no empty arguments list to be transmitted.
     * @param args All positional arguments
     * @return An ArrayNode containing positional arguments or null
     */
    ArrayNode buildArgumentsArray(Object... args) {
        if (args.length == 0) return null;
        // Build the arguments array and serialize the arguments
        final ArrayNode argArray = objectMapper.createArrayNode();
        for (Object arg : args) {
            argArray.addPOJO(arg);
        }
        return argArray;
    }
    
    private void clearPendingRequests(Throwable e) {
        for (Entry<Long, RequestMapEntry> entry : requestMap.entrySet()) {
            entry.getValue().resultSubject.onError(e);
        }
        requestMap.clear();
    }
    
    private void clearAllSubscriptions(Throwable e) {
        for (Entry<String, SubscriptionMapEntry> entry : subscriptionsByUri.entrySet()) {
            for (Subscriber<? super PubSubData> s : entry.getValue().subscribers) {
                if (e == null) s.onCompleted();
                else s.onError(e);
            }
            entry.getValue().state = PubSubState.Unsubscribed;
        }
        subscriptionsByUri.clear();
        subscriptionsBySubscriptionId.clear();
    }
    
    private void clearAllRegisteredProcedures(Throwable e) {
        for (Entry<String, RegisteredProceduresMapEntry> entry : registeredProceduresByUri.entrySet()) {
            if (e == null) entry.getValue().subscriber.onCompleted();
            else entry.getValue().subscriber.onError(e);
            entry.getValue().state = RegistrationState.Unregistered;
        }
        registeredProceduresByUri.clear();
        registeredProceduresById.clear();
    }
    
    /**
     * Publishes an event under the given topic.
     * @param topic The topic that should be used for publishing the event
     * @param args A list of all positional arguments of the event to publish.
     * These will be get serialized according to the Jackson library serializing
     * behavior.
     * @return An observable that provides a notification whether the event
     * publication was successful. This contains either a single value (the
     * publication ID) and will then be completed or will be completed with
     * an error if the event could not be published.
     */
    public Observable<Long> publish(final String topic, Object... args) {
        return publish(topic, buildArgumentsArray(args), null);
    }

    /**
     * Publishes an event under the given topic.
     * @param topic The topic that should be used for publishing the event
     * @param event The event to publish
     * @return An observable that provides a notification whether the event
     * publication was successful. This contains either a single value (the
     * publication ID) and will then be completed or will be completed with
     * an error if the event could not be published.
     */
    public Observable<Long> publish(final String topic, PubSubData event) {
        if (event != null)
            return publish(topic, event.arguments, event.keywordArguments);
        else
            return publish(topic, null, null);
    }

    /**
     * Publishes an event under the given topic.
     * @param topic The topic that should be used for publishing the event
     * @param arguments The positional arguments for the published event
     * @param argumentsKw The keyword arguments for the published event.
     * These will only be taken into consideration if arguments is not null.
     * @return An observable that provides a notification whether the event
     * publication was successful. This contains either a single value (the
     * publication ID) and will then be completed or will be completed with
     * an error if the event could not be published.
     */
    public Observable<Long> publish(final String topic, final ArrayNode arguments, final ObjectNode argumentsKw)
    {
        return publish(topic, null, arguments, argumentsKw);
    }

    /**
     * Publishes an event under the given topic.
     * @param topic The topic that should be used for publishing the event
     * @param options A WAMP options dictionary. May contain advanced options
     * for the publish call.
     * @param arguments The positional arguments for the published event
     * @param argumentsKw The keyword arguments for the published event.
     * These will only be taken into consideration if arguments is not null.
     * @return An observable that provides a notification whether the event
     * publication was successful. This contains either a single value (the
     * publication ID) and will then be completed or will be completed with
     * an error if the event could not be published.
     */
    public Observable<Long> publish(final String topic, final ObjectNode options, final ArrayNode arguments,
        final ObjectNode argumentsKw)
    {
        final AsyncSubject<Long> resultSubject = AsyncSubject.create();
        
        try {
            UriValidator.validate(topic, useStrictUriValidation);
        }
        catch (WampError e) {
            resultSubject.onError(e);
            return resultSubject;
        }
         
        eventLoop.execute(new Runnable() {
            @Override
            public void run() {
                if (status != Status.Connected) {
                    resultSubject.onError(new ApplicationError(ApplicationError.NOT_CONNECTED));
                    return;
                }
                
                final long requestId = IdGenerator.newLinearId(lastRequestId, requestMap);
                lastRequestId = requestId;
                final WampMessages.PublishMessage msg =
                    new WampMessages.PublishMessage(requestId, options, topic, arguments, argumentsKw);
                
                requestMap.put(requestId, new RequestMapEntry(WampMessages.PublishMessage.ID, resultSubject));
                channel.writeAndFlush(msg);
            }
        });
        return resultSubject;
    }
    
    /**
     * Registers a procedure at the router which will afterwards be available
     * for remote procedure calls from other clients.<br>
     * The actual registration will only happen after the user subscribes on
     * the returned Observable. This guarantees that no RPC requests get lost.
     * Incoming RPC requests will be pushed to the Subscriber via it's
     * onNext method. The Subscriber can send responses through the methods on
     * the {@link Request}.<br>
     * If the client no longer wants to provide the method it can call
     * unsubscribe() on the Subscription to unregister the procedure.<br>
     * If the connection closes onCompleted will be called.<br>
     * In case of errors during subscription onError will be called.
     * @param topic The name of the procedure which this client wants to
     * provide.<br>
     * Must be valid WAMP URI.
     * @return An observable that can be used to provide a procedure.
     */
    public Observable<Request> registerProcedure(final String topic) {
        return Observable.create(new OnSubscribe<Request>() {
            @Override
            public void call(final Subscriber<? super Request> subscriber) {
                try {
                    UriValidator.validate(topic, useStrictUriValidation);
                }
                catch (WampError e) {
                    subscriber.onError(e);
                    return;
                }

                eventLoop.execute(new Runnable() {
                    @Override
                    public void run() {
                        // If the Subscriber unsubscribed in the meantime we return early
                        if (subscriber.isUnsubscribed()) return;
                        // Set subscription to completed if we are not connected
                        if (status != Status.Connected) {
                            subscriber.onCompleted();
                            return;
                        }

                        final RegisteredProceduresMapEntry entry = registeredProceduresByUri.get(topic);
                        // Check if we have already registered a function with the same name
                        if (entry != null) {
                            subscriber.onError(
                                new ApplicationError(ApplicationError.PROCEDURE_ALREADY_EXISTS));
                            return;
                        }
                        
                        // Insert a new entry in the subscription map
                        final RegisteredProceduresMapEntry newEntry = 
                            new RegisteredProceduresMapEntry(subscriber, RegistrationState.Registering);
                        registeredProceduresByUri.put(topic, newEntry);

                        // Make the subscribe call
                        final long requestId = IdGenerator.newLinearId(lastRequestId, requestMap);
                        lastRequestId = requestId;
                        final RegisterMessage msg = new RegisterMessage(requestId, null, topic);

                        final AsyncSubject<Long> registerFuture = AsyncSubject.create();
                        registerFuture
                        .observeOn(WampClient.this.scheduler)
                        .subscribe(new Action1<Long>() {
                            @Override
                            public void call(Long t1) {
                                // Check if we were unsubscribed (through transport close)
                                if (newEntry.state != RegistrationState.Registering) return;
                                // Registration at the broker was successful
                                newEntry.state = RegistrationState.Registered;
                                newEntry.registrationId = t1;
                                registeredProceduresById.put(t1, newEntry);
                                // Add the cancellation functionality to the subscriber
                                attachCancelRegistrationAction(subscriber, newEntry, topic);
                            }
                        }, new Action1<Throwable>() {
                            @Override
                            public void call(Throwable t1) {
                                // Error on registering
                                if (newEntry.state != RegistrationState.Registering) return;
                                // Remark: Actually noone can't unregister until this Future completes because
                                // the unregister functionality is only added in the success case
                                // However a transport close event could set us to Unregistered early
                                newEntry.state = RegistrationState.Unregistered;

                                boolean isClosed = false;
                                if (t1 instanceof ApplicationError &&
                                        ((ApplicationError)t1).uri.equals(ApplicationError.TRANSPORT_CLOSED))
                                    isClosed = true;
                                
                                if (isClosed) subscriber.onCompleted();
                                else subscriber.onError(t1);

                                registeredProceduresByUri.remove(topic);
                            }
                        });

                        requestMap.put(requestId, 
                            new RequestMapEntry(RegisterMessage.ID, registerFuture));
                        channel.writeAndFlush(msg);
                    }
                });
            }
        });
    }
    
    /**
     * Add an action that is added to the subscriber which is executed
     * if unsubscribe is called on a registered procedure.<br>
     * This action will lead to unregistering a provided function at the dealer.
     */
    private void attachCancelRegistrationAction(final Subscriber<? super Request> subscriber,
                                                final RegisteredProceduresMapEntry mapEntry,
                                                final String topic)
    {
        subscriber.add(Subscriptions.create(new Action0() {
            @Override
            public void call() {
                eventLoop.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (mapEntry.state != RegistrationState.Registered) return;
                        
                        mapEntry.state = RegistrationState.Unregistering;
                        registeredProceduresByUri.remove(topic);
                        registeredProceduresById.remove(mapEntry.registrationId);
                        
                        // Make the unregister call
                        final long requestId = IdGenerator.newLinearId(lastRequestId, requestMap);
                        lastRequestId = requestId;
                        final UnregisterMessage msg = new UnregisterMessage(requestId, mapEntry.registrationId);
                        
                        final AsyncSubject<Void> unregisterFuture = AsyncSubject.create();
                        unregisterFuture
                        .observeOn(WampClient.this.scheduler)
                        .subscribe(new Action1<Void>() {
                            @Override
                            public void call(Void t1) {
                                // Unregistration at the broker was successful
                                mapEntry.state = RegistrationState.Unregistered;
                            }
                        }, new Action1<Throwable>() {
                            @Override
                            public void call(Throwable t1) {
                                // Error on unregister
                            }
                        });
                        
                        requestMap.put(requestId, new RequestMapEntry(
                            UnregisterMessage.ID, unregisterFuture));
                        channel.writeAndFlush(msg);
                    }
                });
            }
        }));
    }
    
    /**
     * Returns an observable that allows to subscribe on the given topic.<br>
     * The actual subscription will only be made after subscribe() was called
     * on it.<br>
     * This version of makeSubscription will automatically transform the
     * received events data into the type eventClass and will therefore return
     * a mapped Observable. It will only look at and transform the first
     * argument of the received events arguments, therefore it can only be used
     * for events that carry either a single or no argument.<br>
     * Received publications will be pushed to the Subscriber via it's
     * onNext method.<br>
     * The client can unsubscribe from the topic by calling unsubscribe() on
     * it's Subscription.<br>
     * If the connection closes onCompleted will be called.<br>
     * In case of errors during subscription onError will be called.
     * @param topic The topic to subscribe on.<br>
     * Must be valid WAMP URI.
     * @param eventClass The class type into which the received event argument
     * should be transformed. E.g. use String.class to let the client try to
     * transform the first argument into a String and let the return value of
     * of the call be Observable&lt;String&gt;.
     * @return An observable that can be used to subscribe on the topic.
     */
    public <T> Observable<T> makeSubscription(final String topic, final Class<T> eventClass)
    {
        return makeSubscription(topic).map(new Func1<PubSubData,T>() {
            @Override
            public T call(PubSubData ev) {
                if (eventClass == null || eventClass == Void.class) {
                    // We don't need a value
                    return null;
                }
                
                if (ev.arguments == null || ev.arguments.size() < 1)
                    throw OnErrorThrowable.from(new ApplicationError(ApplicationError.MISSING_VALUE));
                    
                JsonNode eventNode = ev.arguments.get(0);
                if (eventNode.isNull()) return null;
                
                T eventValue;
                try {
                    eventValue = objectMapper.convertValue(eventNode, eventClass);
                } catch (IllegalArgumentException e) {
                    throw OnErrorThrowable.from(new ApplicationError(ApplicationError.INVALID_VALUE_TYPE));
                }
                return eventValue;
            }
        });
    }
    
    /**
     * Returns an observable that allows to subscribe on the given topic.<br>
     * The actual subscription will only be made after subscribe() was called
     * on it.<br>
     * Received publications will be pushed to the Subscriber via it's
     * onNext method.<br>
     * The client can unsubscribe from the topic by calling unsubscribe() on
     * it's Subscription.<br>
     * If the connection closes onCompleted will be called.<br>
     * In case of errors during subscription onError will be called.
     * @param topic The topic to subscribe on.<br>
     * Must be valid WAMP URI.
     * @return An observable that can be used to subscribe on the topic.
     */
    public Observable<PubSubData> makeSubscription(final String topic) {
        return Observable.create(new OnSubscribe<PubSubData>() {
            @Override
            public void call(final Subscriber<? super PubSubData> subscriber) {
                try {
                    UriValidator.validate(topic, useStrictUriValidation);
                }
                catch (WampError e) {
                    subscriber.onError(e);
                    return;
                }

                eventLoop.execute(new Runnable() {
                    @Override
                    public void run() {
                        // If the Subscriber unsubscribed in the meantime we return early
                        if (subscriber.isUnsubscribed()) return;
                        // Set subscription to completed if we are not connected
                        if (status != Status.Connected) {
                            subscriber.onCompleted();
                            return;
                        }

                        final SubscriptionMapEntry entry = subscriptionsByUri.get(topic);

                        if (entry != null) { // We are already subscribed at the dealer
                            entry.subscribers.add(subscriber);
                            if (entry.state == PubSubState.Subscribed) {
                                // Add the cancellation functionality only if we are
                                // already subscribed. If not then this will be added
                                // once subscription is completed
                                attachPubSubCancelationAction(subscriber, entry, topic);
                            }
                        }
                        else { // need to subscribe
                            // Insert a new entry in the subscription map
                            final SubscriptionMapEntry newEntry = new SubscriptionMapEntry(PubSubState.Subscribing);
                            newEntry.subscribers.add(subscriber);
                            subscriptionsByUri.put(topic, newEntry);

                            // Make the subscribe call
                            final long requestId = IdGenerator.newLinearId(lastRequestId, requestMap);
                            lastRequestId = requestId;
                            final SubscribeMessage msg = new SubscribeMessage(requestId, null, topic);

                            final AsyncSubject<Long> subscribeFuture = AsyncSubject.create();
                            subscribeFuture
                            .observeOn(WampClient.this.scheduler)
                            .subscribe(new Action1<Long>() {
                                @Override
                                public void call(Long t1) {
                                    // Check if we were unsubscribed (through transport close)
                                    if (newEntry.state != PubSubState.Subscribing) return;
                                    // Subscription at the broker was successful
                                    newEntry.state = PubSubState.Subscribed;
                                    newEntry.subscriptionId = t1;
                                    subscriptionsBySubscriptionId.put(t1, newEntry);
                                    // Add the cancellation functionality to all subscribers
                                    // If one is already unsubscribed this will immediately call
                                    // the cancellation function for this subscriber
                                    for (Subscriber<? super PubSubData> s : newEntry.subscribers) {
                                        attachPubSubCancelationAction(s, newEntry, topic);
                                    }
                                }
                            }, new Action1<Throwable>() {
                                @Override
                                public void call(Throwable t1) {
                                    // Error on subscription
                                    if (newEntry.state != PubSubState.Subscribing) return;
                                    // Remark: Actually noone can't unsubscribe until this Future completes because
                                    // the unsubscription functionality is only added in the success case
                                    // However a transport close event could set us to Unsubscribed early
                                    newEntry.state = PubSubState.Unsubscribed;

                                    boolean isClosed = false;
                                    if (t1 instanceof ApplicationError &&
                                            ((ApplicationError)t1).uri.equals(ApplicationError.TRANSPORT_CLOSED))
                                        isClosed = true;

                                    for (Subscriber<? super PubSubData> s : newEntry.subscribers) {
                                        if (isClosed) s.onCompleted();
                                        else s.onError(t1);
                                    }

                                    newEntry.subscribers.clear();
                                    subscriptionsByUri.remove(topic);
                                }
                            });

                            requestMap.put(requestId, 
                                    new RequestMapEntry(SubscribeMessage.ID, 
                                            subscribeFuture));
                            channel.writeAndFlush(msg);
                        }
                    }
                });
            }
        });
    }
    
    /**
     * Add an action that is added to the subscriber which is executed
     * if unsubscribe is called. This action will lead to the unsubscription at the
     * broker once the topic subscription at the broker is no longer used by anyone.
     */
    private void attachPubSubCancelationAction(final Subscriber<? super PubSubData> subscriber, 
                                               final SubscriptionMapEntry mapEntry,
                                               final String topic)
    {
        subscriber.add(Subscriptions.create(new Action0() {
            @Override
            public void call() {
                eventLoop.execute(new Runnable() {
                    @Override
                    public void run() {
                        mapEntry.subscribers.remove(subscriber);
                        if (mapEntry.state == PubSubState.Subscribed &&
                            mapEntry.subscribers.size() == 0) 
                        {
                            // We removed the last subscriber and can therefore unsubscribe from the dealer
                            mapEntry.state = PubSubState.Unsubscribing;
                            subscriptionsByUri.remove(topic);
                            subscriptionsBySubscriptionId.remove(mapEntry.subscriptionId);
                            
                            // Make the unsubscribe call
                            final long requestId = IdGenerator.newLinearId(lastRequestId, requestMap);
                            lastRequestId = requestId;
                            final UnsubscribeMessage msg = 
                                    new UnsubscribeMessage(requestId, mapEntry.subscriptionId);
                            
                            final AsyncSubject<Void> unsubscribeFuture = AsyncSubject.create();
                            unsubscribeFuture
                            .observeOn(WampClient.this.scheduler)
                            .subscribe(new Action1<Void>() {
                                @Override
                                public void call(Void t1) {
                                    // Unsubscription at the broker was successful
                                    mapEntry.state = PubSubState.Unsubscribed;
                                }
                            }, new Action1<Throwable>() {
                                @Override
                                public void call(Throwable t1) {
                                    // Error on unsubscription
                                }
                            });
                            
                            requestMap.put(requestId, new RequestMapEntry(
                                UnsubscribeMessage.ID, unsubscribeFuture));
                            channel.writeAndFlush(msg);
                        }
                    }
                });
            }
        }));
    }

    /**
     * Performs a remote procedure call through the router.<br>
     * The function will return immediately, as the actual call will happen
     * asynchronously.
     * @param procedure The name of the procedure to call. Must be a valid WAMP
     * Uri.
     * @param arguments A list of all positional arguments for the procedure call
     * @param argumentsKw All named arguments for the procedure call
     * @return An observable that provides a notification whether the call was
     * was successful and the return value. If the call is successful the
     * returned observable will be completed with a single value (the return value).
     * If the remote procedure call yields an error the observable will be completed
     * with an error.
     */
    public Observable<Reply> call(final String procedure,
                                  final ArrayNode arguments,
                                  final ObjectNode argumentsKw)
    {
        final AsyncSubject<Reply> resultSubject = AsyncSubject.create();
        
        try {
            UriValidator.validate(procedure, useStrictUriValidation);
        }
        catch (WampError e) {
            resultSubject.onError(e);
            return resultSubject;
        }
         
        eventLoop.execute(new Runnable() {
            @Override
            public void run() {
                if (status != Status.Connected) {
                    resultSubject.onError(new ApplicationError(ApplicationError.NOT_CONNECTED));
                    return;
                }
                
                final long requestId = IdGenerator.newLinearId(lastRequestId, requestMap);
                lastRequestId = requestId;
                final CallMessage callMsg = new CallMessage(requestId, null, procedure, 
                                                            arguments, argumentsKw);
                
                requestMap.put(requestId, new RequestMapEntry(CallMessage.ID, resultSubject));
                channel.writeAndFlush(callMsg);
            }
        });
        return resultSubject;
    }
    
    /**
     * Performs a remote procedure call through the router.<br>
     * The function will return immediately, as the actual call will happen
     * asynchronously.
     * @param procedure The name of the procedure to call. Must be a valid WAMP
     * Uri.
     * @param args The list of positional arguments for the remote procedure call.
     * These will be get serialized according to the Jackson library serializing
     * behavior.
     * @return An observable that provides a notification whether the call was
     * was successful and the return value. If the call is successful the
     * returned observable will be completed with a single value (the return value).
     * If the remote procedure call yields an error the observable will be completed
     * with an error.
     */
    public Observable<Reply> call(final String procedure, Object... args)
    {
        // Build the arguments array and serialize the arguments
        return call(procedure, buildArgumentsArray(args), null);
    }
    
    /**
     * Performs a remote procedure call through the router.<br>
     * The function will return immediately, as the actual call will happen
     * asynchronously.<br>
     * This overload of the call function will automatically map the received
     * reply value into the specified Java type by using Jacksons object mapping
     * facilities.<br>
     * Only the first value in the array of positional arguments will be taken
     * into account for the transformation. If multiple return values are required
     * another overload of this function has to be used.<br>
     * If the expected return type is not {@link Void} but the return value array
     * contains no value or if the value in the array can not be deserialized into
     * the expected type the returned {@link Observable} will be completed with
     * an error.
     * @param procedure The name of the procedure to call. Must be a valid WAMP
     * Uri.
     * @param returnValueClass The class of the expected return value. If the function
     * uses no return values Void should be used.
     * @param args The list of positional arguments for the remote procedure call.
     * These will be get serialized according to the Jackson library serializing
     * behavior.
     * @return An observable that provides a notification whether the call was
     * was successful and the return value. If the call is successful the
     * returned observable will be completed with a single value (the return value).
     * If the remote procedure call yields an error the observable will be completed
     * with an error.
     */
    public <T> Observable<T> call(final String procedure, 
                                  final Class<T> returnValueClass, Object... args)
    {
        return call(procedure, buildArgumentsArray(args), null).map(new Func1<Reply,T>() {
            @Override
            public T call(Reply reply) {
                if (returnValueClass == null || returnValueClass == Void.class) {
                    // We don't need a return value
                    return null;
                }
                
                if (reply.arguments == null || reply.arguments.size() < 1)
                    throw OnErrorThrowable.from(new ApplicationError(ApplicationError.MISSING_RESULT));
                    
                JsonNode resultNode = reply.arguments.get(0);
                if (resultNode.isNull()) return null;
                
                T result;
                try {
                    result = objectMapper.convertValue(resultNode, returnValueClass);
                } catch (IllegalArgumentException e) {
                    // The returned exception is an aggregate one. That's not too nice :(
                    throw OnErrorThrowable.from(new ApplicationError(ApplicationError.INVALID_VALUE_TYPE));
                }
                return result;
            }
        });
    }
    
    /**
     * Returns a future that will be completed once the client terminates.<br>
     * This can be used to wait for completion after {@link #close() close} was called.
     */
    public Future<Void> getTerminationFuture() {
        final Promise<Void> p = new Promise<Void>();
        statusObservable.subscribe(new Observer<Status>() {
            @Override
            public void onCompleted() {
                p.resolve(null);
            }

            @Override
            public void onError(Throwable e) {
                p.resolve(null);
            }

            @Override
            public void onNext(Status t) { }
        });
        return p.getFuture();
    }

}
