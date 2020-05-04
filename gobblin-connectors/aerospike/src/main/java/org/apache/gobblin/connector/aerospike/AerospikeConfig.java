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

package org.apache.gobblin.connector.aerospike;

import com.typesafe.config.Config;
import org.apache.gobblin.util.ConfigUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Aerospike configurations
 * Aero1 {
    bootstrapServers = "host1,host2,host3"
    bootstrapServersPort = 4333
    user = test-user
    password = test-password
    retries = 3
    timeoutInMillis = 5000
    tls {
      enabled = false
      hostname = "<provide TLS hostname when TLS is enabled>"
    }
    # these are the properties that will be set in system to facilitate the client connection, etc....
    # <required only when TLS is enabled, providing sameple config here.
    system {
      jdk.tls {
        client.protocols = "TLSv1.2"
      }
      javax.net {
        # disable debugging on prod
        debug = "ssl"
        ssl {
          trustStore = "<.ts location>"
          keyStore = "<.jks location>"
          trustStorePassword = "test-password"
          keyStorePassword = "test-password"
        }
      }
    }
    #config for reading from Aero1
    read {
      timeoutInMillis = 10000
      retries = 6
    }
    #config for writing to Aero1
    write {
      namespace = test-namespace
      set = test-set1
      bin = avro-values
      key_columns = key
      key_columns_separator = ","
      bin_columns = value
      bin_columns_separator = ","
      retries = 4
      timeoutInMillis = 5000
    }
  }
 */
public class AerospikeConfig {

    public final Config config;
    public final Config baseConfig;
    public final Config readConfig;
    public final Config writeConfig;
    public final List<String> hosts;
    public final int port;
    public final boolean isTLSEnabled;
    public final String tlsHost;
    public final String user;
    public final String password;
    public final int maxConnRetries;

    public final String namespace;
    public final String set;
    public final String bin;
    public final List<String> keyColumns;
    public final List<String> binColumns;
    public final String keySeparator;
    public final String binSeparator;
    public final int writeRetires;
    public final int writeTimeoutMillis;


    public AerospikeConfig(Config config) {
        this.config = config;
        baseConfig = config.getConfig(PLATFORM_BASE_CONFIG_PATH);
        readConfig = baseConfig.getConfig(READ_CONFIG_PATH);
        writeConfig = baseConfig.getConfig(WRITE_CONFIG_PATH);

        // Common configs from baseConfig
        hosts = ConfigUtils.getStringList(baseConfig, BOOTSTRAP_SERVERS);
        port = ConfigUtils.getInt(baseConfig, BOOTSTRAP_SERVERS_PORT, BOOTSTRAP_SERVERS_PORT_DEFAULT);
        password = baseConfig.getString(PASSWORD);
        user = baseConfig.getString(USER);
        maxConnRetries = baseConfig.getInt(RETRIES);

        isTLSEnabled = ConfigUtils.getBoolean(baseConfig, TLS_ENABLED, false);
        if (isTLSEnabled) {
            tlsHost = baseConfig.getString(TLS_HOSTNAME);
            setSystemProperties();
        } else {
            // Setting NULL value is required for Aerospike client.
            tlsHost = null;
        }

        // Write config
        namespace = ConfigUtils.getString(writeConfig, NAMESPACE, NAMESPACE_DEFAULT);
        set = ConfigUtils.getString(writeConfig, SET, SET_DEFAULT);
        bin = ConfigUtils.getString(writeConfig, BIN, BIN_DEFAULT);
        keyColumns = writeConfig.hasPath(KEY_COLUMNS) ? ConfigUtils.getStringList(writeConfig, KEY_COLUMNS) : new ArrayList<>();
        binColumns = writeConfig.hasPath(BIN_COLUMNS) ? ConfigUtils.getStringList(writeConfig, BIN_COLUMNS) : new ArrayList<>();
        keySeparator = ConfigUtils.getString(writeConfig, KEY_COLUMNS_SEPARATOR, KEY_COLUMNS_SEPARATOR_DEFAULT);
        binSeparator = ConfigUtils.getString(writeConfig, BIN_COLUMNS_SEPARATOR, BIN_COLUMNS_SEPARATOR_DEFAULT);
        writeRetires = ConfigUtils.getInt(writeConfig, RETRIES, RETRIES_DEFAULT);
        writeTimeoutMillis = ConfigUtils.getInt(writeConfig, TIMEOUT_MILLIS, TIMEOUT_MILLIS_DEFAULT);

        // Read config

    }


    public boolean setSystemProperties() {
        Properties properties = System.getProperties();
        Config systemConfig = baseConfig.hasPath(SYSTEM_CONFIG_PATH) ? baseConfig.getConfig(SYSTEM_CONFIG_PATH) : null;
        if (systemConfig != null) {
            systemConfig.entrySet().forEach(e -> properties.setProperty(e.getKey(), systemConfig.getString(e.getKey())));
        }
        return true;
    }

    public static final String PLATFORM_BASE_CONFIG_PATH = "writer.destination";
    public static final String READ_CONFIG_PATH = "read";
    public static final String WRITE_CONFIG_PATH = "write";
    public static final String SYSTEM_CONFIG_PATH = "system";

    //server config
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String BOOTSTRAP_SERVERS_PORT = "bootstrapServersPort";
    public static final int BOOTSTRAP_SERVERS_PORT_DEFAULT = 3000;

    // Auth
    public static final String USER = "user";
    public static final String PASSWORD = "password";

    // Target dataset config
    public static final String NAMESPACE = "namespace";
    public static final String NAMESPACE_DEFAULT = "test_namespace";

    public static final String SET = "set";
    public static final String SET_DEFAULT = "test_set";

    public static final String BIN = "bin";
    public static final String BIN_DEFAULT = "test_bin";

    // Writer OPS config
    public static final String TIMEOUT_MILLIS = "timeoutInMillis";
    public static final int TIMEOUT_MILLIS_DEFAULT = 10000; // 10 second default timeout

    public static final String RETRIES = "retries";
    public static final int RETRIES_DEFAULT = 3;

    public static final String TLS_ENABLED = "tls.enabled";

    // If SECURE_WRITER is enabled
    public static final String TLS_HOSTNAME = "tls.hostname";

    // Key and Value info
    public static final String KEY_COLUMNS = "key_columns";
    public static final String KEY_COLUMNS_SEPARATOR = "key_columns_separator";
    public static final String KEY_COLUMNS_SEPARATOR_DEFAULT = ",";
    public static final String BIN_COLUMNS = "bin_columns";
    public static final String BIN_COLUMNS_SEPARATOR = "bin_columns_separator";
    public static final String BIN_COLUMNS_SEPARATOR_DEFAULT = ",";

}
