package com.reactiveminds.psi.streams.service;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import spark.Request;
import spark.Response;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static spark.Spark.*;
@Component
class SimpleHttpService implements Runnable{

    static final String ARG_STORE = ":store";
    static final String ARG_KEY = ":key";
    static final String QUERY_PATH = "/query/"+ARG_STORE+"/"+ARG_KEY;

    private static final Logger log = LoggerFactory.getLogger(SimpleHttpService.class);

    @Autowired
    StreamsDataService streamsDataService;
    private void openEndpoints() {
        get(QUERY_PATH, (Request request, Response response) -> {
            String store = request.params(ARG_STORE);
            String key = request.params(ARG_KEY);
            String value = null;
            try {
                log.info("querying store '{}', for base64 key {}",store, key);
                value = streamsDataService.findByKey(store, key);
                if(value != null){
                    log.info("found record of base64 length {}", value.length());
                }
            } catch (Exception e) {
                log.error("== controller error ==", e);
                response.status(500);
                return "Internal error";
            }
            if(value != null){
                response.status(200);
                response.header("Content-Type", "text/plain; charset=utf-8");
                return value;
            }
            else{
                response.status(404);
                return "Not Found!";
            }
        });


    }

    String invokeGet(String path)  {
        try {
            ContentResponse response = httpClient.newRequest(path)
                    .method(HttpMethod.GET)
                    .send();
            return response.getContentAsString();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void startServer() {
        port(advPort);
        ipAddress(advHost);
        openEndpoints();

    }


    void stopServer() {
        stop();
    }

    @Value("${psi.stream.queryListener.host:localhost}")
    private String advHost;
    @Value("${psi.stream.queryListener.port:9999}")
    private int advPort;
    @PostConstruct
    void init() throws Exception {
        new Thread(this, "http-listener").start();
        httpClient = new HttpClient();
        httpClient.setFollowRedirects(false);
        httpClient.start();
        log.info("== API Server up and running: http://{}:{}{} ",advHost, advPort, QUERY_PATH);
    }


    private HttpClient httpClient;
    @PreDestroy
    void destroy() throws Exception {
        stopServer();
        if (httpClient != null) {
            httpClient.stop();
        }
    }
    @Override
    public void run() {
        startServer();
    }
}
