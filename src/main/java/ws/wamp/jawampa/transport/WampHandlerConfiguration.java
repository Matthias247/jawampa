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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class WampHandlerConfiguration {
    
    /** The maximum allowed websocket frame size for WAMP messages */
    final static int MAX_WEBSOCKET_FRAME_SIZE = 16*1024*1024; // 16MB
    
    /** A set that contains all supported websocket subprotocols for WAMP */
    final static Set<String> WAMP_WEBSOCKET_PROTOCOLS_SET;
    
    /** A comma seperated list with all supported websocket subprotocols for WAMP */
    final static String WAMP_WEBSOCKET_PROTOCOLS;
    
    static {
        Set<String> p = new HashSet<String>();
        p.add(Serialization.Json.toString());
        p.add(Serialization.MessagePack.toString());
        WAMP_WEBSOCKET_PROTOCOLS_SET = Collections.unmodifiableSet(p);
        
        StringBuilder b = new StringBuilder();
        boolean first = true;
        for (String prot : p) {
            if (!first) b.append(',');
            first = false;
            b.append(prot);
        }
        WAMP_WEBSOCKET_PROTOCOLS = b.toString();
    }
}
