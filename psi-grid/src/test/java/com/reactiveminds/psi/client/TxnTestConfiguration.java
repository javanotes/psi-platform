package com.reactiveminds.psi.client;

import com.reactiveminds.psi.common.imdg.ClientConfiguration;
import com.reactiveminds.psi.server.loaders.LoaderConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Import({ClientConfiguration.class, KafkaAutoConfiguration.class, LoaderConfiguration.class})
@TestConfiguration
class TxnTestConfiguration {

}
