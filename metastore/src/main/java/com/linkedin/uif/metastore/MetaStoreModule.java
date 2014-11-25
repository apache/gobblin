package com.linkedin.uif.metastore;

import java.util.Properties;
import javax.sql.DataSource;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * A Guice module defining the dependencies used by the metastore module.
 *
 * @author ynli
 */
public class MetaStoreModule extends AbstractModule {

    private final Properties properties;

    public MetaStoreModule(Properties properties) {
        this.properties = properties;
    }

    @Override
    protected void configure() {
        bind(Properties.class)
                .annotatedWith(Names.named("dataSourceProperties"))
                .toInstance(this.properties);
        bind(DataSource.class)
                .toProvider(DataSourceProvider.class);
        bind(JobHistoryStore.class)
                .to(DatabaseJobHistoryStore.class);
    }

}
