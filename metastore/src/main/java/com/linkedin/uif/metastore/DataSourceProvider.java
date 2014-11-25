package com.linkedin.uif.metastore;

import javax.sql.DataSource;
import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import com.linkedin.uif.configuration.ConfigurationKeys;

/**
 * A provider class for {@link javax.sql.DataSource}s.
 *
 * @author ynli
 */
public class DataSourceProvider implements Provider<DataSource> {

    private final Properties properties;

    @Inject
    public DataSourceProvider(@Named("dataSourceProperties") Properties properties) {
        this.properties = properties;
    }

    @Override
    public DataSource get() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(this.properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY));
        if (this.properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY) &&
                this.properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)) {
            config.setUsername(this.properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY));
            config.setPassword(this.properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY));
        }
        return new HikariDataSource(config);
    }
}
