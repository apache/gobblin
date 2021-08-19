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

package org.apache.gobblin.service.modules.db;

import java.util.Objects;

import org.flywaydb.core.Flyway;

import com.google.common.util.concurrent.AbstractIdleService;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * This class initializes and migrates the database schema for Gobblin service.
 *
 * We use Flyway to run the migrations that are defined in resources/org/apache/gobblin/service/db/migration
 * */
@Singleton
@Slf4j
public class ServiceDatabaseManager extends AbstractIdleService {

  private final ServiceDatabaseProvider databaseProvider;

  @Inject
  public ServiceDatabaseManager(ServiceDatabaseProvider databaseProvider) {
    this.databaseProvider = Objects.requireNonNull(databaseProvider);
  }

  @Override
  protected void startUp()
      throws Exception {

    Flyway flyway =
        Flyway.configure().locations("classpath:org/apache/gobblin/service/db/migration").failOnMissingLocations(true)
            .dataSource(databaseProvider.getDatasource())
            // Existing GaaS DBs have state store tables.
            // Flyway will refuse to use such non-empty DBs by default. With baselineOnMigrate(true), it should
            // create new tables, while keeping old ones intact.
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load();

    log.info("Ensuring service database is migrated to latest schema");
    // Start the migration
    flyway.migrate();
  }

  @Override
  protected void shutDown()
      throws Exception {

  }
}
