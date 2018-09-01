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
package org.apache.gobblin.elasticsearch.writer;

import org.apache.gobblin.elasticsearch.typemapping.JsonTypeMapper;


public class ElasticsearchWriterConfigurationKeys {

  private static final String ELASTICSEARCH_WRITER_PREFIX = "writer.elasticsearch";

  private static String prefix(String value) { return ELASTICSEARCH_WRITER_PREFIX + "." + value;};

  public static final String ELASTICSEARCH_WRITER_SETTINGS = prefix("settings");
  public static final String ELASTICSEARCH_WRITER_HOSTS = prefix("hosts");
  public static final String ELASTICSEARCH_WRITER_INDEX_NAME = prefix("index.name");
  public static final String ELASTICSEARCH_WRITER_INDEX_TYPE = prefix("index.type");
  public static final String ELASTICSEARCH_WRITER_TYPEMAPPER_CLASS = prefix("typeMapperClass");
  public static final String ELASTICSEARCH_WRITER_TYPEMAPPER_CLASS_DEFAULT = JsonTypeMapper.class.getCanonicalName();
  public static final String ELASTICSEARCH_WRITER_ID_MAPPING_ENABLED = prefix("useIdFromData");
  public static final Boolean ELASTICSEARCH_WRITER_ID_MAPPING_DEFAULT = false;
  public static final String ELASTICSEARCH_WRITER_ID_FIELD = prefix("idFieldName");
  public static final String ELASTICSEARCH_WRITER_ID_FIELD_DEFAULT = "id";
  public static final String ELASTICSEARCH_WRITER_CLIENT_TYPE = prefix("client.type");
  public static final String ELASTICSEARCH_WRITER_CLIENT_TYPE_DEFAULT = "REST";
  public static final String ELASTICSEARCH_WRITER_CLIENT_THREADPOOL_SIZE = prefix("client.threadPoolSize");
  public static final int ELASTICSEARCH_WRITER_CLIENT_THREADPOOL_DEFAULT = 5;
  public static final String ELASTICSEARCH_WRITER_SSL_ENABLED=prefix("ssl.enabled");
  public static final boolean ELASTICSEARCH_WRITER_SSL_ENABLED_DEFAULT=false;
  public static final String ELASTICSEARCH_WRITER_SSL_KEYSTORE_TYPE=prefix("ssl.keystoreType");
  public static final String ELASTICSEARCH_WRITER_SSL_KEYSTORE_TYPE_DEFAULT = "pkcs12";
  public static final String ELASTICSEARCH_WRITER_SSL_KEYSTORE_PASSWORD=prefix("ssl.keystorePassword");
  public static final String ELASTICSEARCH_WRITER_SSL_KEYSTORE_LOCATION=prefix("ssl.keystoreLocation");
  public static final String ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_TYPE=prefix("ssl.truststoreType");
  public static final String ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_TYPE_DEFAULT = "jks";
  public static final String ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_LOCATION=prefix("ssl.truststoreLocation");
  public static final String ELASTICSEARCH_WRITER_SSL_TRUSTSTORE_PASSWORD=prefix("ssl.truststorePassword");
  public static final String ELASTICSEARCH_WRITER_MALFORMED_DOC_POLICY = prefix("malformedDocPolicy");
  public static final String ELASTICSEARCH_WRITER_MALFORMED_DOC_POLICY_DEFAULT = "FAIL";

  //Async Writer Configuration
  public static final String RETRIES_ENABLED = prefix("retriesEnabled");
  public static final boolean RETRIES_ENABLED_DEFAULT = true;
  public static final String MAX_RETRIES = prefix("maxRetries");
  public static final int MAX_RETRIES_DEFAULT = 5;
  static final String FAILURE_ALLOWANCE_PCT_CONFIG = prefix("failureAllowancePercentage");
  static final double FAILURE_ALLOWANCE_PCT_DEFAULT = 0.0;

  public enum ClientType {
    TRANSPORT,
    REST
  }

  public static final String ELASTICSEARCH_WRITER_DEFAULT_HOST = "localhost";
  public static final int ELASTICSEARCH_TRANSPORT_WRITER_DEFAULT_PORT = 9300;
  public static final int ELASTICSEARCH_REST_WRITER_DEFAULT_PORT = 9200;
}
