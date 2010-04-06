package com.github.bjornno.dbfile;

import org.apache.camel.Exchange;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.impl.DefaultProducer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBFileProducer extends DefaultProducer {
    private JdbcTemplate jdbcTemplate;
    private static String SQL_INSERT = "insert into FILE (ID, FILE_NAME, FILE_CONTENT) values (?,?, ?)";

    public DBFileProducer(DBFileEndpoint endpoint, JdbcTemplate jdbcTemplate) {
        super(endpoint);
        this.jdbcTemplate = jdbcTemplate;
        if (endpoint.getTableName() != null) {
            SQL_INSERT = "insert into " + endpoint.getTableName() + " (ID, FILE_NAME, FILE_CONTENT) values (?,?, ?)";
        }
    }

    public void process(final Exchange exchange) throws Exception {
        final GenericFile genericFile = (GenericFile) exchange.getIn().getBody();
        final InputStream fileStream  = new FileInputStream((File) genericFile.getFile());
        final String filKey = exchange.getExchangeId();
        final String fileName = genericFile.getFileName();

        jdbcTemplate.execute(SQL_INSERT, new AbstractLobCreatingPreparedStatementCallback(new DefaultLobHandler()) {
            protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
                ps.setString(1, filKey);
                ps.setString(2, fileName);
                byte[] filasbyte = new byte[(int) genericFile.getFileLength()];
                try {
                    fileStream.read(filasbyte);
                } catch (IOException e) {
                    throw new RuntimeException("");
                }
                lobCreator.setBlobAsBytes(ps, 3, filasbyte);
            }
        });
    }
}