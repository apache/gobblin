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

package org.apache.gobblin.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.util.ConfigUtils;


@Test
public class ServiceRequesterSerDerTest {

  public void testSerDerWithEmptyRequester() throws IOException {
    List<ServiceRequester> list = new ArrayList<>();

    String serialize = RequesterService.serialize(list);
    Properties props = new Properties();

    props.put(RequesterService.REQUESTER_LIST, serialize);

    Config initConfig = ConfigBuilder.create().build();
    Config config = initConfig.withFallback(ConfigFactory.parseString(props.toString()).resolve());

    Properties props2 = ConfigUtils.configToProperties(config);
    String serialize2 = props2.getProperty(RequesterService.REQUESTER_LIST);

    // This may not hold true unless we use/write a json comparator
    // Assert.assertEquals(serialize2, serialize);
    List<ServiceRequester> list2 = RequesterService.deserialize(serialize);
    Assert.assertEquals(list2, list);
  }

  public void testSerDerWithConfig() throws IOException {
    ServiceRequester sr1 = new ServiceRequester("kafkaetl", "user", "dv");
    ServiceRequester sr2 = new ServiceRequester("gobblin", "group", "dv");
    ServiceRequester sr3 = new ServiceRequester("crm-backend", "service", "cert");

    List<ServiceRequester> list = new ArrayList<>();
    sr1.getProperties().put("customKey", "${123}");
    list.add(sr1);
    list.add(sr2);
    list.add(sr3);

    String serialize = RequesterService.serialize(list);
    Properties props = new Properties();

    props.put(RequesterService.REQUESTER_LIST, serialize);

    // config creation must happen this way because in FlowConfigResourceLocalHandler we read the flowconfig like this
    Config initConfig = ConfigBuilder.create().build();
    Config config = initConfig.withFallback(ConfigFactory.parseString(props.toString()).resolve());

    Properties props2 = ConfigUtils.configToProperties(config);
    String serialize2 = props2.getProperty(RequesterService.REQUESTER_LIST);

    // This may not hold true unless we use/write a json comparator
    // Assert.assertEquals(serialize2, serialize);
    List<ServiceRequester> list2 = RequesterService.deserialize(serialize);
    Assert.assertEquals(list2, list);
  }

  public void testOldSerde() throws IOException {
    // test for backward compatibility
    String serialized = "W3sibmFtZSI6ImNocmxpIiwidHlwZSI6IlVTRVJfUFJJTkNJUEFMIiwiZnJvbSI6ImR2X3Rva2VuIiwicHJvcGVydGllcyI6e319XQ%3D%3D";
    List<ServiceRequester> list = RequesterService.deserialize(serialized);
    List<ServiceRequester> list2 = Collections.singletonList(new ServiceRequester("chrli", "USER_PRINCIPAL", "dv_token"));
    Assert.assertEquals(list, list2);
  }
}
