package com.github.bjornno.dbfile;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.util.CamelContextHelper;
import org.apache.camel.util.IntrospectionSupport;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Map;


public class DBFileComponent extends DefaultComponent {
    private DataSource dataSource;

    public DBFileComponent() {
    }

    public DBFileComponent(CamelContext context) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        String dataSourceRef = getAndRemoveParameter(parameters, "dataSourceRef", String.class);
        if (dataSourceRef != null) {
            dataSource = CamelContextHelper.mandatoryLookup(getCamelContext(), dataSourceRef, DataSource.class);
        }
        
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        IntrospectionSupport.setProperties(jdbcTemplate, parameters, "template.");

        String tableName = remaining;

        return new DBFileEndpoint(uri, this, tableName, jdbcTemplate);
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
