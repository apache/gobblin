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

package org.apache.gobblin.runtime.job_spec;

import java.net.URI;

import org.apache.gobblin.runtime.api.JobCatalogWithTemplates;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SecureJobTemplate;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class JobSpecResolverTest {

	@Test
	public void test() throws Exception {

		Config sysConfig = ConfigFactory.empty();

		JobTemplate jobTemplate = Mockito.mock(JobTemplate.class);
		Mockito.when(jobTemplate.getResolvedConfig(Mockito.any(Config.class))).thenAnswer(i -> {
			Config userConfig = (Config) i.getArguments()[0];
			return ConfigFactory.parseMap(ImmutableMap.of("template.value", "foo")).withFallback(userConfig);
		});

		JobCatalogWithTemplates catalog = Mockito.mock(JobCatalogWithTemplates.class);
		Mockito.when(catalog.getTemplate(Mockito.eq(URI.create("my://template")))).thenReturn(jobTemplate);

		JobSpecResolver resolver = JobSpecResolver.builder(sysConfig).jobCatalog(catalog).build();
		JobSpec jobSpec = JobSpec.builder()
				.withConfig(ConfigFactory.parseMap(ImmutableMap.of("key", "value")))
				.withTemplate(URI.create("my://template")).build();
		ResolvedJobSpec resolvedJobSpec = resolver.resolveJobSpec(jobSpec);

		Assert.assertEquals(resolvedJobSpec.getOriginalJobSpec(), jobSpec);
		Assert.assertEquals(resolvedJobSpec.getConfig().entrySet().size(), 2);
		Assert.assertEquals(resolvedJobSpec.getConfig().getString("key"), "value");
		Assert.assertEquals(resolvedJobSpec.getConfig().getString("template.value"), "foo");
	}

	@Test
	public void testWithResolutionAction() throws Exception {

		Config sysConfig = ConfigFactory.empty();

		SecureJobTemplate insecureTemplate = Mockito.mock(SecureJobTemplate.class);
		Mockito.when(insecureTemplate.getResolvedConfig(Mockito.any(Config.class))).thenAnswer(i -> (Config) i.getArguments()[0]);
		Mockito.when(insecureTemplate.isSecure()).thenReturn(false);

		SecureJobTemplate secureTemplate = Mockito.mock(SecureJobTemplate.class);
		Mockito.when(secureTemplate.getResolvedConfig(Mockito.any(Config.class))).thenAnswer(i -> (Config) i.getArguments()[0]);
		Mockito.when(secureTemplate.isSecure()).thenReturn(true);

		JobCatalogWithTemplates catalog = Mockito.mock(JobCatalogWithTemplates.class);
		Mockito.when(catalog.getTemplate(Mockito.eq(URI.create("my://template.insecure")))).thenReturn(insecureTemplate);
		Mockito.when(catalog.getTemplate(Mockito.eq(URI.create("my://template.secure")))).thenReturn(secureTemplate);

		JobSpecResolver resolver = JobSpecResolver.builder(sysConfig).jobCatalog(catalog)
				// This resolution action should block any resolution that does not use a secure template
				.jobResolutionAction(new SecureTemplateEnforcer()).build();

		JobSpec jobSpec = JobSpec.builder()
				.withConfig(ConfigFactory.parseMap(ImmutableMap.of("key", "value")))
				.withTemplate(URI.create("my://template.insecure")).build();
		Assert.expectThrows(JobTemplate.TemplateException.class, () -> resolver.resolveJobSpec(jobSpec));

		JobSpec jobSpec2 = JobSpec.builder()
				.withConfig(ConfigFactory.parseMap(ImmutableMap.of("key", "value")))
				.withTemplate(URI.create("my://template.secure")).build();
		ResolvedJobSpec resolvedJobSpec = resolver.resolveJobSpec(jobSpec2);

		Assert.assertEquals(resolvedJobSpec.getOriginalJobSpec(), jobSpec2);
		Assert.assertEquals(resolvedJobSpec.getConfig().entrySet().size(), 1);
		Assert.assertEquals(resolvedJobSpec.getConfig().getString("key"), "value");
	}


}
