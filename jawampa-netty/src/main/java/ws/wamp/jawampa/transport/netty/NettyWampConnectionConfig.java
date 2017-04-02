package ws.wamp.jawampa.transport.netty;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import ws.wamp.jawampa.connection.IWampClientConnectionConfig;

public class NettyWampConnectionConfig implements IWampClientConnectionConfig {

    static final int DEFAULT_MAX_FRAME_PAYLOAD_LENGTH = 65535;

    SslContext sslContext;
    int maxFramePayloadLength;
    HttpHeaders httpHeaders;

    NettyWampConnectionConfig(SslContext sslContext, int maxFramePayloadLength, HttpHeaders httpHeaders) {
        this.sslContext = sslContext;
        this.maxFramePayloadLength = maxFramePayloadLength;
        this.httpHeaders = httpHeaders;
    }

    /**
     * the SslContext which will be used to create Ssl connections to the WAMP
     * router. If this is set to null a default (unsecure) SSL client context will be created
     * and used. 
     */
    public SslContext sslContext() {
        return sslContext;
    }

    public int getMaxFramePayloadLength() {
        return maxFramePayloadLength;
    }

    public HttpHeaders getHttpHeaders() {
        return httpHeaders;
    }

    /**
     * Builder class that must be used to create a {@link NettyWampConnectionConfig}
     * instance.
     */
    public static class Builder {

        SslContext sslContext;
        int maxFramePayloadLength = DEFAULT_MAX_FRAME_PAYLOAD_LENGTH;
        HttpHeaders httpHeaders = new DefaultHttpHeaders();

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

        public Builder withMaxFramePayloadLength(int maxFramePayloadLength){
            if ( maxFramePayloadLength <= 0 ){
                throw new IllegalArgumentException("maxFramePayloadLength parameter cannot be negative");
            }
            this.maxFramePayloadLength = maxFramePayloadLength;
            return this;
        }

        /**
         * Add a new header with the specified name and value.
         * @param name The name of the header being added.
         * @param value The value of the header being added.
         * @return The {@link Builder} object.
         */
        public Builder withHttpHeader(String name, Object value) {
            this.httpHeaders.add(name, value);
            return this;
        }

        public NettyWampConnectionConfig build() {
            return new NettyWampConnectionConfig(sslContext, maxFramePayloadLength, httpHeaders);
        }
    }
}
