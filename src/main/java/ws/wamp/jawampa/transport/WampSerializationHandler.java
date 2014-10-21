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

import java.util.List;

import ws.wamp.jawampa.WampMessages.WampMessage;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public class WampSerializationHandler extends MessageToMessageEncoder<WampMessage> {
    
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WampSerializationHandler.class);
    
    enum ReadState {
        Closed,
        Reading,
        Error
    }
    
    final ObjectMapper objectMapper;
    final Serialization serialization;
    
    public Serialization serialization() {
        return serialization;
    }
    
    public WampSerializationHandler(Serialization serialization, ObjectMapper objectMapper) {
        this.serialization = serialization;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, WampMessage msg, List<Object> out) throws Exception {
        if (serialization == Serialization.Json) {
            ByteBuf msgBuffer = Unpooled.buffer();
            ByteBufOutputStream outStream = new ByteBufOutputStream(msgBuffer);
            try {
                objectMapper.writeValue(outStream, msg.toObjectArray(objectMapper)); // TODO: Non JSON Mapping
            } catch (Exception e) {
                msgBuffer.release();
                return;
            }
            
            TextWebSocketFrame frame = new TextWebSocketFrame(msgBuffer);
            if (logger.isDebugEnabled()) {
                logger.debug("Serialized Wamp Message: {}", frame.text());
            }
            //System.out.println("Serialized Wamp message: " + frame.text());
            out.add(frame);
        }
    }
}
