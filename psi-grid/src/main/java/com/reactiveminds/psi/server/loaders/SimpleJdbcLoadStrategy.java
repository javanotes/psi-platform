package com.reactiveminds.psi.server.loaders;

import com.reactiveminds.psi.server.KeyLoadStrategy;
import com.reactiveminds.psi.common.JsonUtils;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.Assert;

import java.util.Properties;

class SimpleJdbcLoadStrategy implements KeyLoadStrategy<SpecificRecord, SpecificRecord> {
    private String queryStr;

    SimpleJdbcLoadStrategy(String queryStr, Class<? extends SpecificRecord> valueTyp) {
        this.queryStr = queryStr;
        this.valueTyp = valueTyp;
    }

    SimpleJdbcLoadStrategy(){

    }
    public String getQueryStr() {
        return queryStr;
    }

    public void setQueryStr(String queryStr) {
        this.queryStr = queryStr;
    }

    public Class<? extends SpecificRecord> getValueTyp() {
        return valueTyp;
    }

    public void setValueTyp(Class<? extends SpecificRecord> valueTyp) {
        this.valueTyp = valueTyp;
    }

    @Autowired
    NamedParameterJdbcTemplate jdbcTemplate;
    private Class<? extends SpecificRecord> valueTyp;
    @Override
    public SpecificRecord findByKey(SpecificRecord key) throws Exception {
        String jsonDoc = jdbcTemplate.queryForObject(queryStr, new BeanPropertySqlParameterSource(key), String.class);
        return JsonUtils.mapper().reader(valueTyp).readValue(jsonDoc);
    }

    @Override
    public void initialize(Properties props) {
        String query = props.getProperty("key.load.strategy.jdbc.queryStr");
        Assert.notNull(query, "Property not set `key.load.strategy.jdbc.queryStr`. mapstore: "+props.getProperty("map"));
        String valueType = props.getProperty("key.load.strategy.jdbc.valueTyp");
        Assert.notNull(valueType, "Property not set `key.load.strategy.valueTyp`. mapstore: "+props.getProperty("map"));
        Class<? extends SpecificRecord> ofType = null;
        try {
            ofType = (Class<? extends SpecificRecord>) Class.forName(valueType);
        } catch (ClassNotFoundException | ClassCastException e) {
            Assert.notNull(ofType, e.getMessage()+". mapstore: "+props.getProperty("map"));
        }

        setQueryStr(query);
        setValueTyp(ofType);
    }
}
