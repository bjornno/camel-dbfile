package com.github.bjornno.dbfile;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.springframework.jdbc.core.JdbcTemplate;


public class DBFileEndpoint extends DefaultEndpoint {
    private JdbcTemplate jdbcTemplate;
    private String tableName;

    public DBFileEndpoint() {
    }

    public DBFileEndpoint(String uri, Component component, String tableName, JdbcTemplate jdbcTemplate) {
        super(uri, component);
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Producer createProducer() throws Exception {
        return new DBFileProducer(this, tableName, jdbcTemplate);
    }

    public boolean isSingleton() {
        return true;
    }      
}
