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
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.typesafe.config.Config;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A factory to hand out {@link com.couchbase.client.java.env.CouchbaseEnvironment} instances
 */
public class CouchbaseEnvironmentFactory {

  private static CouchbaseEnvironment couchbaseEnvironment = null;

  /**
   * Currently hands out a singleton DefaultCouchbaseEnvironment.
   * This is done because it is recommended to use a single couchbase environment instance per JVM.
   * TODO: Determine if we need to use the config to tweak certain parameters
   * @param config
   * @return
   */
  public static synchronized CouchbaseEnvironment getInstance(Config config)
  {
    Boolean sslEnabled = ConfigUtils.getBoolean(config, CouchbaseWriterConfigurationKeys.SSL_ENABLED, false);
    String sslKeystoreFile = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.SSL_KEYSTORE_FILE, "");
    String sslKeystorePassword = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.SSL_KEYSTORE_PASSWORD, "");
    String sslTruststoreFile = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.SSL_TRUSTSTORE_FILE, "");
    String sslTruststorePassword = ConfigUtils.getString(config, CouchbaseWriterConfigurationKeys.SSL_TRUSTSTORE_PASSWORD, "");
    Boolean certAuthEnabled = ConfigUtils.getBoolean(config, CouchbaseWriterConfigurationKeys.CERT_AUTH_ENABLED, false);
    Boolean dnsSrvEnabled = ConfigUtils.getBoolean(config, CouchbaseWriterConfigurationKeys.DNS_SRV_ENABLED, false);
    Integer socketConnectTimeout = ConfigUtils.getInt(config, CouchbaseWriterConfigurationKeys.SOCKET_CONNECT_TIMEOUT,
        DefaultCouchbaseEnvironment.SOCKET_CONNECT_TIMEOUT);

    DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder()
        .sslEnabled(sslEnabled)
        .sslKeystoreFile(sslKeystoreFile)
        .sslKeystorePassword(sslKeystorePassword)
        .sslTruststoreFile(sslTruststoreFile)
        .sslTruststorePassword(sslTruststorePassword)
        .certAuthEnabled(certAuthEnabled)
        .dnsSrvEnabled(dnsSrvEnabled)
        .socketConnectTimeout(socketConnectTimeout);

    if (couchbaseEnvironment == null)
    {
      couchbaseEnvironment = builder.build();
    }
    return couchbaseEnvironment;
  }
}
