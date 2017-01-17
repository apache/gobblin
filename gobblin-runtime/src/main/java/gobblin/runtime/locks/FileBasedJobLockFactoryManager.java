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
package gobblin.runtime.locks;

import java.io.IOException;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alias;
import gobblin.runtime.instance.hadoop.HadoopConfigLoader;

/**
 * Manages instances of {@link FileBasedJobLockFactory}.
 */
@Alias(value="file")
public class FileBasedJobLockFactoryManager
       extends AbstractJobLockFactoryManager<FileBasedJobLock, FileBasedJobLockFactory> {
  public static final String CONFIG_PREFIX = "job.lock.file";

  @Override
  protected Config getFactoryConfig(Config sysConfig) {
    return sysConfig.hasPath(CONFIG_PREFIX) ?
        sysConfig.getConfig(CONFIG_PREFIX) :
        ConfigFactory.empty();
  }

  @Override
  protected FileBasedJobLockFactory createFactoryInstance(final Optional<Logger> log,
                                                          final Config sysConfig,
                                                          final Config factoryConfig) {
    HadoopConfigLoader hadoopConfLoader = new HadoopConfigLoader(sysConfig);
    try {
      return FileBasedJobLockFactory.create(factoryConfig, hadoopConfLoader.getConf(), log);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create job lock factory: " +e, e);
    }
  }

}
