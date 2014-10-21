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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A factory that can create channel objects that are used by
 * {@link ws.wamp.jawampa.WampClient WampClients} to connect to WAMP routers.
 */
public interface WampClientChannelFactory {

    /**
     * Creates a new transport towards the given URI
     * @param handler The clients session handler that should be put on top of the new pipeline.
     * @param eventLoop The EventLoopGroup that the new channel should be assigned to.
     * @param objectMapper The ObjectMapper that the transport should
     * use for serialization.
     * @return A {@link ChannelFuture} that will complete when the transport
     * was created. If an error happens during channel creation the future will be completed
     * with an error. In this case the channel member won't be valid. 
     */
    ChannelFuture createChannel(ChannelHandler handler, EventLoopGroup eventLoop, ObjectMapper objectMapper) 
        throws Exception;
}
