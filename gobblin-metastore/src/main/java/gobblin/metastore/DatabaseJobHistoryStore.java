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

package gobblin.metastore;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import gobblin.metastore.database.SupportedDatabaseVersion;
import gobblin.metastore.database.VersionedDatabaseJobHistoryStore;
import gobblin.rest.JobExecutionInfo;
import gobblin.rest.JobExecutionQuery;

import org.reflections.util.ClasspathHelper;


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

  private static VersionedDatabaseJobHistoryStore findVersionedDatabaseJobHistoryStore(MigrationVersion requiredVersion)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    Class<?> foundClazz = null;
    Class<?> defaultClazz = null;
    MigrationVersion defaultVersion = MigrationVersion.EMPTY;
    // Scan all packages
    Reflections reflections = new Reflections("gobblin.metastore.database",
        effectiveClassPathUrls(DatabaseJobHistoryStore.class.getClassLoader()));
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
