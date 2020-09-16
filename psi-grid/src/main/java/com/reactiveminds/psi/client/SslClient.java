package com.reactiveminds.psi.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.util.function.Supplier;

class SslClient implements Supplier<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(SslClient.class);
    @Value("${psi.grid.sslListenerPort:9000}")
    int port;
    @Value("${psi.grid.sslListenerHost:localhost}")
    String host;

    @Override
    public Boolean get() {
        SSLSocketFactory sslSocketFactory =
                (SSLSocketFactory)SSLSocketFactory.getDefault();
        try (SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket(host, port)){
            socket.startHandshake();
            return true;
        } catch (IOException ex) {
            log.warn(ex.getMessage());
        }
        return false;
    }
}
