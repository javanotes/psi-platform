package com.reactiveminds.psi;

import com.reactiveminds.psi.client.ClientConfiguration;
import com.reactiveminds.psi.server.ServerConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Import;

@Import({ClientConfiguration.class, ServerConfiguration.class})
@SpringBootConfiguration
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
