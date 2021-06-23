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

import java.util.Properties;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Setter;
import lombok.experimental.Accessors;


/**
 * A helper class to build Config for Elasticsearch Writers
 */
@Accessors(chain=true)
public class ConfigBuilder {
  @Setter
  String indexName;
  @Setter
  String indexType;
  @Setter
  int httpPort;
  @Setter
  int transportPort;
  @Setter
  boolean idMappingEnabled = true;
  @Setter
  String clientType = "REST";
  @Setter
  String typeMapperClassName;
  @Setter
  MalformedDocPolicy malformedDocPolicy;

  Config build() {
    Properties props = new Properties();
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_CLIENT_TYPE, clientType);
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_NAME, indexName);
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_TYPE, indexType);
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_ID_MAPPING_ENABLED,
        "" + idMappingEnabled);
    if (this.clientType.equalsIgnoreCase("rest")) {
      props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_HOSTS, "localhost:" + httpPort);
    } else if (this.clientType.equalsIgnoreCase("transport")) {
      props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_HOSTS, "localhost:" + transportPort);
    } else throw new RuntimeException("Client type needs to be one of rest/transport");
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_TYPEMAPPER_CLASS, typeMapperClassName);
    if (malformedDocPolicy != null) {
      props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_MALFORMED_DOC_POLICY,
          malformedDocPolicy.toString().toUpperCase());
    }
    return ConfigFactory.parseProperties(props);
  }
}
