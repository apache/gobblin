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

package org.apache.gobblin.runtime;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.runtime.cli.CliApplication;
import org.apache.gobblin.runtime.cli.CliObjectFactory;
import org.apache.gobblin.runtime.cli.CliObjectSupport;
import org.apache.gobblin.runtime.cli.ConstructorAndPublicMethodsCliObjectFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.gobblin.configuration.ConfigurationKeys.*;


/**
 * A script used for state store migration:
 * In the case that users are willing to change the storage medium of job state due to some reasons.
 *
 * Current implementation doesn't support data awareness on either source or target side.
 * And only migrate a single job state instead of migrating all history versions.
 */
@Slf4j
@Alias(value = "stateMigration", description = "Command line tools for migrating state store")
public class StateStoreMigrationCli implements CliApplication {
  private static final String SOURCE_KEY = "source";
  private static final String DESTINATION_KEY = "destination";
  private static final String JOB_NAME_KEY = "jobName";

  /**
   * Assume that there's no additional '/' in the end of state.store.dir's value
   */
  private String extractStoreName(String dirPath) {
    return dirPath.substring(dirPath.lastIndexOf('/') + 1);
  }

  @Override
  public void run(String[] args) throws Exception {
    CliObjectFactory<Command> factory = new ConstructorAndPublicMethodsCliObjectFactory<>(Command.class);
    Command command = factory.buildObject(args, 1, true, args[0]);

    FileSystem fs = FileSystem.get(new Configuration());
    FSDataInputStream inputStream = fs.open(command.path);
    Config config = ConfigFactory.parseReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
    String storeName = this.extractStoreName(config.getConfig(SOURCE_KEY).getString(STATE_STORE_ROOT_DIR_KEY));

    Preconditions.checkNotNull(config.getObject(SOURCE_KEY));
    Preconditions.checkNotNull(config.getObject(DESTINATION_KEY));
    Preconditions.checkNotNull(config.getString(JOB_NAME_KEY));

    DatasetStateStore dstDatasetStateStore =
        DatasetStateStore.buildDatasetStateStore(config.getConfig(DESTINATION_KEY));
    DatasetStateStore srcDatasetStateStore = DatasetStateStore.buildDatasetStateStore(config.getConfig(SOURCE_KEY));

    Map<String, JobState.DatasetState> map =
        srcDatasetStateStore.getLatestDatasetStatesByUrns(config.getString(JOB_NAME_KEY));
    for (Map.Entry<String, JobState.DatasetState> entry : map.entrySet()) {
      dstDatasetStateStore.persistDatasetState(entry.getKey(), entry.getValue());
    }
    if (command.deleteSourceStateStore) {
      try {
        srcDatasetStateStore.delete(storeName);
      } catch (IOException ioe) {
        log.warn("The source state store has been deleted.", ioe);
      }
    }
  }

  /**
   * This class has to been public static for being accessed by
   * {@link ConstructorAndPublicMethodsCliObjectFactory#inferConstructorOptions}
   */
  public static class Command {

    private final Path path;
    private boolean deleteSourceStateStore = false;

    @CliObjectSupport(argumentNames = "configPath")
    public Command(String path) throws URISyntaxException, IOException {
      this.path = new Path(path);
    }

    public void deleteSourceStateStore() {
      this.deleteSourceStateStore = true;
    }
  }
}
