package com.reactiveminds.psi.server.loaders;

import com.reactiveminds.psi.server.KeyLoadStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@EnableConfigurationProperties(DataSourceProperties.class)
@Configuration
public class LoaderConfiguration {
    @Autowired
    DataSourceProperties properties;

    @Bean
    NamedParameterJdbcOperations operations() {
        return new NamedParameterJdbcTemplate(jdbcTemplate());
    }

    @Bean
    PlatformTransactionManager transactionManager() {
        return new DataSourceTransactionManager(cpDatasource());
    }

    @Bean
    DataSource cpDatasource() {
        return properties.initializeDataSourceBuilder().build();
    }

    @Bean
    JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(cpDatasource());
    }

    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @ConditionalOnProperty(name = "key.load.strategy", havingValue = "jdbc")
    @Bean
    KeyLoadStrategy keyLoadStrategy(){
        return new SimpleJdbcLoadStrategy();
    }

    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @ConditionalOnProperty(name = "key.load.strategy", havingValue = "ktable", matchIfMissing = true)
    @Bean
    KeyLoadStrategy kafkaKeyLoadStrategy(){
        return new KafkaTableLoadStrategy();
    }

}
