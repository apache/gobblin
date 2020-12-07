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
package org.apache.gobblin.metastore.util;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;


/**
 * A utility class that wraps the {@link StateStoreCleaner} implementation as a {@link Runnable}.
 */
@Slf4j
@Deprecated
public class StateStoreCleanerRunnable implements Runnable {
  private Properties properties;

  public StateStoreCleanerRunnable(Config config) {
    this.properties = ConfigUtils.configToProperties(config);
  }

  public void run() {
    Closer closer = Closer.create();
    try {
      log.info("Attempting to clean state store..");
      closer.register(new StateStoreCleaner(properties)).run();
      log.info("State store clean up successful.");
    } catch (IOException | ExecutionException e) {
      log.error("Exception encountered during execution of {}", StateStoreCleaner.class.getName());
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        log.error("Exception when closing the closer", e);
      }
    }
  }
}
