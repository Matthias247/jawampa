package ws.wamp.jawampa.transport.netty;

import io.netty.handler.ssl.SslContext;
import ws.wamp.jawampa.connection.IWampClientConnectionConfig;

public class NettyWampConnectionConfig implements IWampClientConnectionConfig {
    SslContext sslContext;
    
    NettyWampConnectionConfig(SslContext sslContext) {
        this.sslContext = sslContext;
    }
    
    /**
     * the SslContext which will be used to create Ssl connections to the WAMP
     * router. If this is set to null a default (unsecure) SSL client context will be created
     * and used. 
     */
    public SslContext sslContext() {
        return sslContext;
    }
    
    /**
     * Builder class that must be used to create a {@link NettyWampConnectionConfig}
     * instance.
     */
    public static class Builder {
        
        SslContext sslContext;
    
        /**
         * Allows to set the SslContext which will be used to create Ssl connections to the WAMP
         * router. If this is set to null a default (unsecure) SSL client context will be created
         * and used. 
         * @param sslContext The SslContext that will be used for SSL connections.
         * @return The {@link Builder} object
         */
        public Builder withSslContext(SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }
        
        public NettyWampConnectionConfig build() {
            return new NettyWampConnectionConfig(sslContext);
        }
    }
}
