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
package org.apache.gobblin.runtime.instance.plugin.hadoop;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.std.DefaultConfigurableImpl;

import avro.shaded.com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link HadoopKerberosKeytabAuthenticationPlugin}
 */
public class TestHadoopKerberosKeytabAuthenticationPlugin {

  @Test
  public void testConstructor() {
    final Config testConfig = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("hadoop-inject.hadoop.security.authentication", "simple")
        .put("gobblin.instance.hadoop.loginUser", "foo")
        .put("gobblin.instance.hadoop.loginUserKeytabFile", "/tmp/bar")
        .build());
    GobblinInstanceDriver instance = Mockito.mock(GobblinInstanceDriver.class);
    Mockito.when(instance.getSysConfig()).thenReturn(DefaultConfigurableImpl.createFromConfig(testConfig));
    HadoopKerberosKeytabAuthenticationPlugin plugin = (HadoopKerberosKeytabAuthenticationPlugin)
        (new HadoopKerberosKeytabAuthenticationPlugin.ConfigBasedFactory()).createPlugin(instance);
    Assert.assertEquals(plugin.getLoginUser(), "foo");
    Assert.assertEquals(plugin.getLoginUserKeytabFile(), "/tmp/bar");
    Assert.assertEquals(plugin.getHadoopConf().get("hadoop.security.authentication"), "simple");
  }

  @Test
  public void testConfigConstructor() {
    final Config testConfig = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("hadoop-inject.hadoop.security.authentication", "simple")
        .put("gobblin.instance.hadoop.loginUser", "foo")
        .put("gobblin.instance.hadoop.loginUserKeytabFile", "/tmp/bar")
        .build());
    HadoopKerberosKeytabAuthenticationPlugin plugin = (HadoopKerberosKeytabAuthenticationPlugin)
        (new HadoopKerberosKeytabAuthenticationPlugin.ConfigBasedFactory()).createPlugin(testConfig);
    Assert.assertEquals(plugin.getLoginUser(), "foo");
    Assert.assertEquals(plugin.getLoginUserKeytabFile(), "/tmp/bar");
    Assert.assertEquals(plugin.getHadoopConf().get("hadoop.security.authentication"), "simple");
  }

  @Test
  public void testMissingOptions() {
    final Config testConfig1 = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("hadoop-inject.hadoop.security.authentication", "simple")
        .put("hadoop.loginUser", "foo")
        .put("gobblin.instance.hadoop.loginUserKeytabFile", "/tmp/bar")
        .build());
    final GobblinInstanceDriver instance1 = Mockito.mock(GobblinInstanceDriver.class);
    Mockito.when(instance1.getSysConfig()).thenReturn(DefaultConfigurableImpl.createFromConfig(testConfig1));

    Assert.assertThrows(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        (new HadoopKerberosKeytabAuthenticationPlugin.ConfigBasedFactory()).createPlugin(instance1);
      }
    });

    final Config testConfig2 = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("hadoop-inject.hadoop.security.authentication", "simple")
        .put("gobblin.instance.hadoop.loginUser", "foo")
        .put("hadoop.loginUserKeytabFile", "/tmp/bar")
        .build());
    final GobblinInstanceDriver instance2 = Mockito.mock(GobblinInstanceDriver.class);
    Mockito.when(instance1.getSysConfig()).thenReturn(DefaultConfigurableImpl.createFromConfig(testConfig2));

    Assert.assertThrows(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        (new HadoopKerberosKeytabAuthenticationPlugin.ConfigBasedFactory()).createPlugin(instance2);
      }
    });

  }

}
