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

package ws.wamp.jawampa.transport;

import java.net.URI;

import javax.net.ssl.SSLException;

import ws.wamp.jawampa.ApplicationError;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Returns factory methods for the establishment of WAMP connections between
 * clients and routers.<br>
 */
public class WampClientChannelFactoryResolver {
    
    public static WampClientChannelFactory getFactory(final URI uri, final SslContext sslCtx) throws ApplicationError
    {
        String scheme = uri.getScheme();
        scheme = scheme != null ? scheme : "";
        
        if (scheme.equalsIgnoreCase("ws") || scheme.equalsIgnoreCase("wss")) {
            
            // Check the host and port field for validity
            if (uri.getHost() == null || uri.getPort() == 0) {
                throw new ApplicationError(ApplicationError.INVALID_URI);
            }
            
            // Return a factory that creates a channel for websocket connections            
            return new WampClientChannelFactory() {
                @Override
                public ChannelFuture createChannel(final ChannelHandler handler, 
                                                   final EventLoopGroup eventLoop)
                                                   throws Exception
                {
                    // Initialize SSL when required
                    final boolean needSsl = uri.getScheme().equalsIgnoreCase("wss");
                    final SslContext sslCtx0;
                    if (needSsl && sslCtx == null) {
                        // Create a default SslContext when we got none provided through the constructor
                        try {
                            sslCtx0 = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
                        }
                        catch (SSLException e) {
                            throw e;
                        }
                    } else if (needSsl) {
                        sslCtx0 = sslCtx;
                    } else {
                        sslCtx0 = null;
                    }
                    
                    // Use well-known ports if not explicitly specified
                    final int port;
                    if (uri.getPort() == -1) {
                        if (needSsl) port = 443;
                        else port = 80;
                    } else port = uri.getPort();
                    
                    final WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                            uri, WebSocketVersion.V13, WampHandlerConfiguration.WAMP_WEBSOCKET_PROTOCOLS,
                            false, new DefaultHttpHeaders());
                    
                    Bootstrap b = new Bootstrap();
                    b.group(eventLoop)
                     .channel(NioSocketChannel.class)
                     .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (sslCtx0 != null) {
                            p.addLast(sslCtx0.newHandler(ch.alloc(), 
                                                         uri.getHost(),
                                                         port));
                        }
                        p.addLast(
                            new HttpClientCodec(),
                            new HttpObjectAggregator(8192),
                            new WebSocketClientProtocolHandler(handshaker, false),
                            new WebSocketFrameAggregator(WampHandlerConfiguration.MAX_WEBSOCKET_FRAME_SIZE),
                            new WampClientWebsocketHandler(handshaker),
                            handler);
                        }
                    });
                    
                    return b.connect(uri.getHost(), port);
                }
            };
        }
        
        throw new ApplicationError(ApplicationError.INVALID_URI);
    }
}
