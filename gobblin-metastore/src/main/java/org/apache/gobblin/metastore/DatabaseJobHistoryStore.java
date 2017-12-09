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

package org.apache.gobblin.metastore;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.gobblin.metastore.database.SupportedDatabaseVersion;
import org.apache.gobblin.metastore.database.VersionedDatabaseJobHistoryStore;
import org.apache.gobblin.rest.JobExecutionInfo;
import org.apache.gobblin.rest.JobExecutionQuery;

import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;


/**
 * An implementation of {@link JobHistoryStore} backed by MySQL.
 *
 * <p>
 *     The DDLs for the MySQL job history store can be found under metastore/src/main/resources.
 * </p>
 *
 * @author Yinan Li
 */
public class DatabaseJobHistoryStore implements JobHistoryStore {
  // Scan all packages in the classpath with prefix org.apache.gobblin.metastore.database when
  // class is loaded. Since scan is expensive we do it only once when class is loaded.
  private static final Reflections reflections = new Reflections(getConfigurationBuilder());
  private final VersionedDatabaseJobHistoryStore versionedStore;

  @Inject
  public DatabaseJobHistoryStore(DataSource dataSource)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    MigrationVersion databaseVersion = getDatabaseVersion(dataSource);
    this.versionedStore = findVersionedDatabaseJobHistoryStore(databaseVersion);
    this.versionedStore.init(dataSource);
  }

  @Override
  public synchronized void put(JobExecutionInfo jobExecutionInfo) throws IOException {
    this.versionedStore.put(jobExecutionInfo);
  }

  @Override
  public synchronized List<JobExecutionInfo> get(JobExecutionQuery query) throws IOException {
    return this.versionedStore.get(query);
  }

  @Override
  public void close() throws IOException {
    this.versionedStore.close();
  }

  private static MigrationVersion getDatabaseVersion(DataSource dataSource) throws FlywayException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(dataSource);
    MigrationInfoService info = flyway.info();
    MigrationVersion currentVersion = MigrationVersion.EMPTY;
    if (info.current() != null) {
      currentVersion = info.current().getVersion();
    }
    return currentVersion;
  }

  private static Collection<URL> effectiveClassPathUrls(ClassLoader... classLoaders) {
    return ClasspathHelper.forManifest(ClasspathHelper.forClassLoader(classLoaders));
  }

  private static Configuration getConfigurationBuilder() {
    ConfigurationBuilder configurationBuilder=  ConfigurationBuilder.build("org.apache.gobblin.metastore.database",
        effectiveClassPathUrls(DatabaseJobHistoryStore.class.getClassLoader()));
    List<URL> filteredUrls = Lists.newArrayList(Iterables.filter(configurationBuilder.getUrls(), new Predicate<URL>() {
      @Override
      public boolean apply(@Nullable URL input) {
        return input != null && (!input.getProtocol().equals("file") || new File(input.getFile()).exists());
      }
    }));
    configurationBuilder.setUrls(filteredUrls);
    return configurationBuilder;
  }

  private static VersionedDatabaseJobHistoryStore findVersionedDatabaseJobHistoryStore(MigrationVersion requiredVersion)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    Class<?> foundClazz = null;
    Class<?> defaultClazz = null;
    MigrationVersion defaultVersion = MigrationVersion.EMPTY;
    for (Class<?> clazz : Sets.intersection(reflections.getTypesAnnotatedWith(SupportedDatabaseVersion.class),
            reflections.getSubTypesOf(VersionedDatabaseJobHistoryStore.class))) {
      SupportedDatabaseVersion annotation = clazz.getAnnotation(SupportedDatabaseVersion.class);
      String version = annotation.version();
      MigrationVersion actualVersion = MigrationVersion.fromVersion(Strings.isNullOrEmpty(version) ? null : version);
      if (annotation.isDefault() && actualVersion.compareTo(defaultVersion) > 0) {
        defaultClazz = clazz;
        defaultVersion = actualVersion;
      }
      if (actualVersion.compareTo(requiredVersion) == 0) {
        foundClazz = clazz;
      }
    }
    if (foundClazz == null) {
      foundClazz = defaultClazz;
    }
    if (foundClazz == null) {
      throw new ClassNotFoundException(
          String.format("Could not find an instance of %s which supports database " + "version %s.",
              VersionedDatabaseJobHistoryStore.class.getSimpleName(), requiredVersion.toString()));
    }
    return (VersionedDatabaseJobHistoryStore) foundClazz.newInstance();
  }
}
