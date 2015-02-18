/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore;

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
    bind(Properties.class).annotatedWith(Names.named("dataSourceProperties")).toInstance(this.properties);
    bind(DataSource.class).toProvider(DataSourceProvider.class);
    bind(JobHistoryStore.class).to(DatabaseJobHistoryStore.class);
  }
}
