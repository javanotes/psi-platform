package com.reactiveminds.psi.server.loaders;

import com.reactiveminds.psi.common.err.InternalOperationFailed;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

class SimpleHttpClient implements Runnable{

    public SimpleHttpClient(String uri) {
        this.uri = uri;
    }

    private String uri;

    public String getResponseContent() {
        return responseContent;
    }

    private String responseContent = null;
    private static HttpClient client;
    static {
        System.setProperty("jdk.httpclient.keepalive.timeout", "99999");
        System.setProperty("jdk.httpclient.connectionPoolSize", "20");
        client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }
    @Override
    public void run() {
        HttpRequest request = HttpRequest.newBuilder(URI.create(uri))
                .header("Accept", "application/text")
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if(response.statusCode() == 200)
                responseContent = response.body();

        } catch (IOException e) {
            throw new InternalOperationFailed("http get error", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}
