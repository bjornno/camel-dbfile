package com.github.bjornno.dbfile;

import org.apache.camel.ExchangePattern;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.impl.DefaultMessage;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DBFileConsumer extends ScheduledPollConsumer {
    private JdbcTemplate jdbcTemplate;
    private DBFileEndpoint endpoint;
    private final ScheduledThreadPoolExecutor executor;
    private final int concurrentConsumers = 6;

    public DBFileConsumer(DBFileEndpoint endpoint, Processor processor, JdbcTemplate jdbcTemplate) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.jdbcTemplate = jdbcTemplate;
        this.executor = new ScheduledThreadPoolExecutor(this.concurrentConsumers);
    }

    @Override
    protected void poll() throws Exception {
        List fileids = jdbcTemplate.queryForList("select id from "+endpoint.getTableName() +" where status = 0", String.class);

        for (Iterator iterator = fileids.iterator(); iterator.hasNext();) {
            String fileid = (String) iterator.next();
            Task worker = new Task(endpoint, this.getProcessor(), fileid, jdbcTemplate);
            executor.execute(worker);
        }
    }

    @Override
    protected void doStop() throws Exception {
        executor.shutdown();
    }
}
class Task implements Runnable {
    private DBFileEndpoint endpoint;
    private final Processor processor;
    private final String fileId;
    private JdbcTemplate jdbcTemplate;
    private LobHandler lobHandler;

    public Task(DBFileEndpoint endpoint, Processor processor, String fileId, JdbcTemplate jdbcTemplate) throws Exception {
        this.endpoint = endpoint;
        this.processor = processor;
        this.fileId = fileId;
        this.jdbcTemplate = jdbcTemplate;

    }

    public void run() {
        DefaultExchange exchange = (DefaultExchange) endpoint.createExchange(ExchangePattern.InOut);
        Message message = new DefaultMessage();
        lobHandler = new DefaultLobHandler();
        try {
            List<Reader> list = jdbcTemplate.query("select file_content from file where id= '"+fileId+"'",  new RowMapper() {
                public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return new InputStreamReader(lobHandler.getBlobAsBinaryStream(rs, 1));
                }

            });

            message.setBody(list.get(0));
            exchange.setIn(message);

            processor.process(exchange);
        } catch (Exception e) {
            jdbcTemplate.update("update file set status = 1 where id = '" + fileId + "'");
            System.out.println("Exception :" + e.getMessage());
        }
        jdbcTemplate.update("update file set status = 2 where id = '" + fileId + "'");
    }

}