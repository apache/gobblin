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

package org.apache.gobblin.runtime.api;

import java.net.URI;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SecureJobTemplateTest {

	@Test
	public void test() {

		SecureJobTemplate template = Mockito.mock(SecureJobTemplate.class);
		Mockito.when(template.getUri()).thenReturn(URI.create("my://template"));
		Mockito.when(template.isSecure()).thenReturn(true);
		Mockito.when(template.overridableProperties()).thenReturn(Lists.newArrayList("my.overridable.property1", "my.overridable.property2"));

		// Test simple filtering
		Config config = ConfigFactory.parseMap(ImmutableMap.of(
				"someProperty", "foo",
				"my.overridable.property1", "bar"
		));
		Config result = SecureJobTemplate.filterUserConfig(template, config, log);
		Assert.assertEquals(result.entrySet().size(), 1);
		Assert.assertEquals(result.getString("my.overridable.property1"), "bar");
		Assert.assertFalse(result.hasPath("someProperty"));
		Assert.assertFalse(result.hasPath("my.overridable.property2"));

		// Test allowing override of a subconfig
		config = ConfigFactory.parseMap(ImmutableMap.of(
				"someProperty", "foo",
				"my.overridable.property1.key1", "bar",
				"my.overridable.property1.key2", "baz"
		));
		result = SecureJobTemplate.filterUserConfig(template, config, log);
		Assert.assertEquals(result.entrySet().size(), 2);
		Assert.assertEquals(result.getString("my.overridable.property1.key1"), "bar");
		Assert.assertEquals(result.getString("my.overridable.property1.key2"), "baz");

		// Test multiple overrides
		config = ConfigFactory.parseMap(ImmutableMap.of(
				"someProperty", "foo",
				"my.overridable.property1", "bar",
				"my.overridable.property2", "baz"
		));
		result = SecureJobTemplate.filterUserConfig(template, config, log);
		Assert.assertEquals(result.entrySet().size(), 2);
		Assert.assertEquals(result.getString("my.overridable.property1"), "bar");
		Assert.assertEquals(result.getString("my.overridable.property2"), "baz");
	}

}
