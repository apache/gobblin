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

import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.gobblin.elasticsearch.typemapping.AvroGenericRecordTypeMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ElasticsearchTransportClientWriterTest {

  @Test
  public void testBadSslConfiguration()
      throws UnknownHostException {
    Properties props = new Properties();
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_NAME, "test");
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_INDEX_TYPE, "test");
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_TYPEMAPPER_CLASS,
        AvroGenericRecordTypeMapper.class.getCanonicalName());
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_ID_MAPPING_ENABLED, "true");
    props.setProperty(ElasticsearchWriterConfigurationKeys.ELASTICSEARCH_WRITER_SSL_ENABLED, "true");
    Config config = ConfigFactory.parseProperties(props);
    try {
      new ElasticsearchTransportClientWriter(config);
      Assert.fail("Writer should not be constructed");
    }
    catch (Exception e) {
    }
  }

}
