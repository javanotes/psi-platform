package com.reactiveminds.psi.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;

import javax.annotation.PreDestroy;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import java.io.IOException;

class SslServer  {
    private static final Logger log = LoggerFactory.getLogger(SslServer.class);
    @Value("${ssl.listener.port:9000}")
    int port;
    private volatile boolean running;
    @Async
    public void run() {
        SSLServerSocketFactory sslServerSocketFactory =
                (SSLServerSocketFactory)SSLServerSocketFactory.getDefault();

        try {
            SSLServerSocket sslServerSocket =
                    (SSLServerSocket) sslServerSocketFactory.createServerSocket(port);
            sslServerSocket.setNeedClientAuth(true);
            running = true;
            log.info("SSL authenticator listening on port {}",port);
            while (running) {
                try (SSLSocket s = (SSLSocket) sslServerSocket.accept()){
                    s.startHandshake();
                }
                catch (IOException e) {
                    log.warn(e.getMessage());
                }
            }
            try {
                sslServerSocket.close();
            } catch (IOException e) {

            }

        } catch (IOException ex) {
            log.warn("client ssl authentication will be be skipped! Please install certificate with `-Djavax.net.ssl.keyStore=<keystore> -Djavax.net.ssl.keyStorePassword=<password>`", ex);
        }
    }
    @PreDestroy
    void stop(){
        running = false;
    }
}
