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

package org.apache.gobblin.couchbase.writer;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Properties;
import junit.framework.Assert;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.AsyncWriterManager;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.log4j.Logger;


public class CouchbaseWriterBuilder extends DataWriterBuilder {
  private static final Logger LOG = Logger.getLogger(CouchbaseWriterBuilder.class);
  public DataWriter build(Config config) throws IOException {
    Assert.assertNotNull("Config cannot be null", config);
    config.entrySet().stream().forEach(x -> String.format("Config passed to factory builder '%s':'%s'", x.getKey(), x.getValue().toString()));
    CouchbaseEnvironment couchbaseEnvironment = CouchbaseEnvironmentFactory.getInstance(config);

    //TODO: Read config to decide whether to build a blocking writer or an async writer

    double failureAllowance =
        ConfigUtils.getDouble(config, CouchbaseWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_CONFIG,
            CouchbaseWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_DEFAULT) / 100.0;

    boolean retriesEnabled = ConfigUtils.getBoolean(config, CouchbaseWriterConfigurationKeys.RETRIES_ENABLED,
        CouchbaseWriterConfigurationKeys.RETRIES_ENABLED_DEFAULT);

    int maxRetries = ConfigUtils.getInt(config, CouchbaseWriterConfigurationKeys.MAX_RETRIES,
        CouchbaseWriterConfigurationKeys.MAX_RETRIES_DEFAULT);

    // build an async couchbase writer
    AsyncDataWriter couchbaseWriter = new CouchbaseWriter(couchbaseEnvironment, config);
    return AsyncWriterManager.builder()
        .asyncDataWriter(couchbaseWriter)
        .failureAllowanceRatio(failureAllowance)
        .retriesEnabled(retriesEnabled)
        .numRetries(maxRetries)
        .config(config)
        .build();
  }

  @Override
  public DataWriter build() throws IOException {
    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config config = ConfigUtils.propertiesToConfig(taskProps);
    return build(config);
  }
}
