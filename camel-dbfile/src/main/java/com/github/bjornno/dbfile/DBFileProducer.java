package com.github.bjornno.dbfile;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.springframework.jdbc.core.JdbcTemplate;

public class DBFileProducer extends DefaultProducer {
    private JdbcTemplate jdbcTemplate;
    private String tableName;

    public DBFileProducer(DBFileEndpoint endpoint, String tableName, JdbcTemplate jdbcTemplate) {
        super(endpoint);
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    public void process(final Exchange exchange) throws Exception {
        jdbcTemplate.execute("insert into "+tableName+"(id, text) values('"+exchange.getExchangeId()+"', '"+exchange.getIn().getBody()+"')");
    }

}
