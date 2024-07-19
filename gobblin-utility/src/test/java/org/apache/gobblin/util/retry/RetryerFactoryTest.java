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
package org.apache.gobblin.util.retry;

import java.util.Arrays;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.google.common.base.Predicate;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


/**
 * Unit tests for the {@link org.apache.gobblin.util.retry.RetryerFactory} class.
 */
public class RetryerFactoryTest {

  @Test
  public void testGetRetryPredicateFromConfigOrDefault_withEmptyConfig() {
    Config config = ConfigFactory.empty();
    Predicate<Throwable> result = RetryerFactory.getRetryPredicateFromConfigOrDefault(config);

    Assert.assertEquals(RetryerFactory.RETRY_EXCEPTION_PREDICATE_DEFAULT, result);
  }

  @Test
  public void testGetRetryPredicateFromConfigOrDefault_withValidException() {
    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.EXCEPTION_LIST_FOR_RETRY_CONFIG_KEY,
            ConfigValueFactory.fromAnyRef(Arrays.asList("java.lang.RuntimeException")));

    Predicate<Throwable> result = RetryerFactory.getRetryPredicateFromConfigOrDefault(config);

    Assert.assertTrue(result.test(new RuntimeException()));
    Assert.assertFalse(result.test(new Exception()));
  }

  @Test
  public void testGetRetryPredicateFromConfigOrDefault_withInvalidException() {
    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.EXCEPTION_LIST_FOR_RETRY_CONFIG_KEY,
            ConfigValueFactory.fromAnyRef(Arrays.asList("non.existent.Exception")));

    Predicate<Throwable> result = RetryerFactory.getRetryPredicateFromConfigOrDefault(config);

    Assert.assertEquals(RetryerFactory.RETRY_EXCEPTION_PREDICATE_DEFAULT, result);
  }

  @Test
  public void testGetRetryPredicateFromConfigOrDefault_withMixedExceptions() {
    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.EXCEPTION_LIST_FOR_RETRY_CONFIG_KEY,
            ConfigValueFactory.fromAnyRef(Arrays.asList("java.lang.RuntimeException", "non.existent.Exception")));

    Predicate<Throwable> result = RetryerFactory.getRetryPredicateFromConfigOrDefault(config);

    Assert.assertTrue(result.test(new RuntimeException()));
    Assert.assertFalse(result.test(new Exception()));
  }
}
