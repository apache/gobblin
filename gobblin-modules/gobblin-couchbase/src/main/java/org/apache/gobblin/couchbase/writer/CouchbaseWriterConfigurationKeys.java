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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class CouchbaseWriterConfigurationKeys {

  public static final String COUCHBASE_WRITER_PREFIX="writer.couchbase.";

  private static String prefix(String value) { return COUCHBASE_WRITER_PREFIX + value;};

  public static final String BOOTSTRAP_SERVERS= prefix("bootstrapServers");
  public static final List<String> BOOTSTRAP_SERVERS_DEFAULT= Collections.singletonList("localhost");

  public static final String BUCKET=prefix("bucket");
  public static final String BUCKET_DEFAULT = "default";
  public static final String PASSWORD = prefix("password");

  public static final String SSL_ENABLED = prefix("sslEnabled");
  public static final String SSL_KEYSTORE_FILE = prefix("sslKeystoreFile");
  public static final String SSL_KEYSTORE_PASSWORD = prefix("sslKeystorePassword");
  public static final String SSL_TRUSTSTORE_FILE = prefix("sslTruststoreFile");
  public static final String SSL_TRUSTSTORE_PASSWORD = prefix("sslTruststorePassword");
  public static final String CERT_AUTH_ENABLED = prefix("certAuthEnabled");
  public static final String DNS_SRV_ENABLED = prefix("dnsSrvEnabled");
  public static final String SOCKET_CONNECT_TIMEOUT = prefix("socketConnectTimeout");

  public static final String DOCUMENT_TTL = prefix("documentTTL");
  public static final String DOCUMENT_TTL_UNIT = prefix("documentTTLUnits");
  public static final TimeUnit DOCUMENT_TTL_UNIT_DEFAULT = TimeUnit.SECONDS;
  public static final String DOCUMENT_TTL_ORIGIN_FIELD = prefix("documentTTLOriginField");
  public static final String DOCUMENT_TTL_ORIGIN_FIELD_UNITS = prefix("documentTTLOriginUnits");
  public static final TimeUnit DOCUMENT_TTL_ORIGIN_FIELD_UNITS_DEFAULT = TimeUnit.MILLISECONDS;

  public static final String OPERATION_TIMEOUT_MILLIS = prefix("operationTimeoutMillis");
  public static final long OPERATION_TIMEOUT_DEFAULT = 10000; // 10 second default timeout

  public static final String RETRIES_ENABLED = prefix("retriesEnabled");
  public static final boolean RETRIES_ENABLED_DEFAULT = false;

  public static final String MAX_RETRIES = prefix("maxRetries");
  public static final int MAX_RETRIES_DEFAULT = 5;

  static final String FAILURE_ALLOWANCE_PCT_CONFIG = prefix("failureAllowancePercentage");
  static final double FAILURE_ALLOWANCE_PCT_DEFAULT = 0.0;

}
