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
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public class WampDeserializationHandler extends MessageToMessageDecoder<WebSocketFrame> {
    
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WampDeserializationHandler.class);
    
    enum ReadState {
        Closed,
        Reading,
        Error
    }
    
    ReadState readState = ReadState.Closed;
    
    final ObjectMapper objectMapper;
    final Serialization serialization;
    
    public Serialization serialization() {
        return serialization;
    }
    
    public WampDeserializationHandler(Serialization serialization, ObjectMapper objectMapper) {
        this.serialization = serialization;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        readState = ReadState.Reading;
    }
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        readState = ReadState.Reading;
        ctx.fireChannelActive();
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        readState = ReadState.Closed;
        ctx.fireChannelInactive();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) 
        throws Exception 
    {
        if (readState != ReadState.Reading) return;
        if (frame instanceof TextWebSocketFrame) {
            // Only want Text when JSON subprotocol
            if (serialization != Serialization.Json)
                throw new IllegalStateException("Received unexpected TextFrame");
            
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            if (logger.isDebugEnabled()) {
                logger.debug("Deserialized Wamp Message: {}", textFrame.text());
            }
            //System.out.println("Deserialized Wamp message: " + textFrame.text());
            
            try {
                // If we receive an invalid frame on of the following functions will throw
                // This will lead Netty to closing the connection
                ArrayNode arr = objectMapper.readValue(
                    new ByteBufInputStream(textFrame.content()), ArrayNode.class);
            
                WampMessage recvdMessage = WampMessage.fromObjectArray(arr);
                out.add(recvdMessage);
            } finally {
            }
        } else if (frame instanceof BinaryWebSocketFrame) {
            // Only want Binary when MessagePack subprotocol
            if (serialization != Serialization.MessagePack)
                throw new IllegalStateException("Received unexpected BinaryFrame");
        
            // TODO: Support MessagePack
        } else if (frame instanceof PongWebSocketFrame) {
            // System.out.println("WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            // System.out.println("WebSocket Client received closing");
            readState = ReadState.Closed;
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // We caught an exception.
        // Most probably because we received an invalid message
        readState = ReadState.Error;
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
           .addListener(ChannelFutureListener.CLOSE);
    }
}
