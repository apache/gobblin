/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.metastore.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.google.common.io.Closer;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang.StringUtils;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.info.MigrationInfoDumper;

import gobblin.metastore.DatabaseJobHistoryStore;


/**
 * A utility class for managing the {@link DatabaseJobHistoryStore} schema.
 *
 * @author Joel Baranick
 */
public class DatabaseJobHistoryStoreSchemaManager implements Closeable {

  private final Flyway flyway;

  private DatabaseJobHistoryStoreSchemaManager(Properties properties) {
    flyway = new Flyway();
    flyway.configure(properties);
    flyway.setClassLoader(this.getClass().getClassLoader());
  }

  public static DataSourceBuilder builder() {
      return new Builder();
  }

  private static FinalBuilder builder(Properties properties) {
    return new Builder(properties);
  }

  public void migrate() throws FlywayException {
    flyway.migrate();
  }

  public void info() throws FlywayException {
    MigrationInfoService info = flyway.info();
    System.out.println(MigrationInfoDumper.dumpToAsciiTable(info.all()));
  }

  @Override
  public void close() throws IOException {
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1 || args.length > 2) {
      printUsage();
    }
    Closer closer = Closer.create();
    try {
      CompositeConfiguration config = new CompositeConfiguration();
      config.addConfiguration(new SystemConfiguration());
      if (args.length == 2) {
        config.addConfiguration(new PropertiesConfiguration(args[1]));
      }
      Properties properties = getProperties(config);
      DatabaseJobHistoryStoreSchemaManager schemaManager =
              closer.register(DatabaseJobHistoryStoreSchemaManager.builder(properties).build());
      if (String.CASE_INSENSITIVE_ORDER.compare("migrate", args[0]) == 0) {
        schemaManager.migrate();
      } else if (String.CASE_INSENSITIVE_ORDER.compare("info", args[0]) == 0) {
        schemaManager.info();
      } else {
        printUsage();
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private static void printUsage() {
    System.err.println("Usage: migrate|info [configuration file]");
    System.exit(1);
  }

  private static Properties getProperties(Configuration config) {
    Properties props = new Properties();
    char delimiter = (config instanceof AbstractConfiguration)
              ? ((AbstractConfiguration) config).getListDelimiter() : ',';
      Iterator keys = config.getKeys();
      while (keys.hasNext())
      {
        String key = (String) keys.next();
        List list = config.getList(key);

        props.setProperty("flyway." + key, StringUtils.join(list.iterator(), delimiter));
      }
      return props;
  }

  public interface DataSourceBuilder {
    VersionBuilder setDataSource(String url, String user, String password);
  }

  public interface VersionBuilder extends FinalBuilder {
    FinalBuilder setVersion(String version);
  }

  public interface FinalBuilder {
    DatabaseJobHistoryStoreSchemaManager build();
  }

  private static class Builder implements DataSourceBuilder, VersionBuilder, FinalBuilder {
    private final Properties properties;

    public Builder() {
      properties = new Properties();
    }

    public Builder(Properties properties) {
      this.properties = properties;
    }

    @Override
    public DatabaseJobHistoryStoreSchemaManager build() {
      return new DatabaseJobHistoryStoreSchemaManager(properties);
    }

    @Override
    public VersionBuilder setDataSource(String url, String user, String password) {
      this.properties.setProperty("flyway.url", url);
      this.properties.setProperty("flyway.user", user);
      this.properties.setProperty("flyway.password", password);
      return this;
    }

    @Override
    public FinalBuilder setVersion(String version) {
      if (!"latest".equalsIgnoreCase(version)) {
        this.properties.setProperty("flyway.target", version);
      }
      return this;
    }
  }
}
