package com.github.bjornno.dbfile;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.Reader;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class RoutesTest {
    JdbcTemplate jdbcTemplate;
    DataSource dataSource;

    @Before
    public void setup() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
        dataSource.setUrl("jdbc:hsqldb:mem:test");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        this.dataSource = dataSource;
        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("create table file (id varchar, file_name varchar, file_content BINARY, status integer default 0)");
    }
    @Test
    public void testRoute() throws Exception {
        assertEquals("wrong number before file", 0, jdbcTemplate.queryForLong("select count(*) from file"));
        final AtomicBoolean invoked = new AtomicBoolean();
        CamelContext context = new DefaultCamelContext();
        DBFileComponent dbFileComponent = new DBFileComponent();
        dbFileComponent.setDataSource(dataSource);
        context.addComponent("dbfile", dbFileComponent);
        context.addRoutes(createFileInAndOutRoute(invoked));
        context.start();
        Thread.currentThread().sleep(4000);  // todo: hmmm, really must find away to avoid this..
        assertEquals("file not inserted in db", 1, jdbcTemplate.queryForLong("select count(*) from file"));
        assertTrue("file not retrieved from db", invoked.get());
    }



    private RouteBuilder createFileInAndOutRoute(final AtomicBoolean invoked) {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("file:src/test/resources?noop=true").to("dbfile:file");

                from("dbfile:file").process(new Processor() {
                    public void process(Exchange exchange) throws Exception {
                        Reader r = (Reader) exchange.getIn().getBody();
                        BufferedReader bufread = new BufferedReader(r);

                        String line = bufread.readLine();
                        int i = 0;
                        while (line != null) {
                            // System.out.println("");
                            i++;
                            line = bufread.readLine();
                        }
                        if (i >0) {
                            invoked.set(true);
                        }
                    }
                }) ;
            }
        };
    }


}
