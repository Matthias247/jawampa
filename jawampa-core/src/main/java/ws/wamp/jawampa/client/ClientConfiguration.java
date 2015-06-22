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

package ws.wamp.jawampa.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import ws.wamp.jawampa.WampRoles;
import ws.wamp.jawampa.auth.client.ClientSideAuthentication;
import ws.wamp.jawampa.connection.IWampConnector;
import ws.wamp.jawampa.connection.IWampConnectorProvider;

/**
 * Stores various configuration data for WAMP clients
 */
public class ClientConfiguration {
    final boolean closeClientOnErrors;
    
    final String authId;
    final List<ClientSideAuthentication> authMethods;
    
    final ObjectMapper objectMapper = new ObjectMapper();

    final URI routerUri;
    final String realm;
    final boolean useStrictUriValidation;
    
    final WampRoles[] clientRoles;
    
    final int totalNrReconnects;
    final int reconnectInterval;
    
    /** The provider that should be used to obtain a connector */
    final IWampConnectorProvider connectorProvider;
    /** The connector which is used to create new connections to the remote peer */
    final IWampConnector connector;
    
    public ClientConfiguration(
        boolean closeClientOnErrors,
        String authId,
        List<ClientSideAuthentication> authMethods,
        URI routerUri,
        String realm,
        boolean useStrictUriValidation,
        WampRoles[] clientRoles,
        int totalNrReconnects,
        int reconnectInterval,
        IWampConnectorProvider connectorProvider,
        IWampConnector connector)
    {
        this.closeClientOnErrors = closeClientOnErrors;
        
        this.authId = authId;
        this.authMethods = authMethods;
        
        this.routerUri = routerUri;
        this.realm = realm;
        
        this.useStrictUriValidation = useStrictUriValidation;
        
        this.clientRoles = clientRoles;
        
        this.totalNrReconnects = totalNrReconnects;
        this.reconnectInterval = reconnectInterval;
        
        this.connectorProvider = connectorProvider;
        this.connector = connector;
    }
    
    public boolean closeClientOnErrors() {
        return closeClientOnErrors;
    }
    
    public ObjectMapper objectMapper() {
        return objectMapper;
    }
    
    public URI routerUri() {
        return routerUri;
    }
    
    public String realm() {
        return realm;
    }
    
    public boolean useStrictUriValidation() {
        return useStrictUriValidation;
    }
    
    public int totalNrReconnects() {
        return totalNrReconnects;
    }
    
    public int reconnectInterval() {
        return reconnectInterval;
    }
    
    /** The connector which is used to create new connections to the remote peer */
    public IWampConnector connector() {
        return connector;
    }

    public WampRoles[] clientRoles() {
        return clientRoles.clone();
    }
    
    public String authId() {
        return authId;
    }
    
    public List<ClientSideAuthentication> authMethods() {
        return new ArrayList<ClientSideAuthentication>(authMethods);
    }
}
