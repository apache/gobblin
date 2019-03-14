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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.JobCatalog;
import org.apache.gobblin.runtime.api.JobCatalogWithTemplates;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.job_catalog.InMemoryJobCatalog;
import org.apache.gobblin.runtime.job_catalog.PackagedTemplatesJobCatalogDecorator;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;


/**
 * Resolved {@link JobSpec}s into {@link ResolvedJobSpec}s, this means:
 * - Applying the template to obtain the final job config.
 * - Calling resolution callbacks.
 */
@AllArgsConstructor
public class JobSpecResolver {

	public static final String JOB_RESOLUTION_ACTIONS_KEY = "org.apache.gobblin.jobResolution.actions";

	private final JobCatalogWithTemplates jobCatalog;
	private final List<JobResolutionCallbacks> jobResolutionCallbacks;
	private final Config sysConfig;

	/**
	 * Obtain a mock {@link JobSpecResolver} with an empty configuration. It does not run any callbacks.
	 * @return
	 */
	public static JobSpecResolver mock() {
		try {
			return new Builder().sysConfig(ConfigFactory.empty()).build();
		} catch (IOException ioe) {
			throw new RuntimeException("Unexpected error. This is an error in code.", ioe);
		}
	}

	/**
	 * @return a Builder for {@link JobSpecResolver}.
	 */
	public static Builder builder(GobblinInstanceDriver driver) throws IOException {
		return new Builder().sysConfig(driver.getSysConfig().getConfig()).jobCatalog(driver.getJobCatalog());
	}

	/**
	 * @return a Builder for {@link JobSpecResolver}.
	 */
	public static Builder builder(Config sysConfig) throws IOException {
		return new Builder().sysConfig(sysConfig);
	}

	/**
	 * Used for building {@link JobSpecResolver}.
	 */
	public static class Builder {
		private JobCatalogWithTemplates jobCatalog = new PackagedTemplatesJobCatalogDecorator(new InMemoryJobCatalog());
		private List<JobResolutionCallbacks> jobResolutionCallbacks = new ArrayList<>();
		private Config sysConfig;

		private Builder sysConfig(Config sysConfig) throws IOException {
			this.sysConfig = sysConfig;
			for (String action : Splitter.on(",").trimResults().omitEmptyStrings()
					.split(ConfigUtils.getString(sysConfig, JOB_RESOLUTION_ACTIONS_KEY, ""))) {
				try {
					ClassAliasResolver<JobResolutionCallbacks> resolver = new ClassAliasResolver<>(JobResolutionCallbacks.class);
					JobResolutionCallbacks actionInstance = resolver.resolveClass(action).newInstance();
					this.jobResolutionCallbacks.add(actionInstance);
				} catch (ReflectiveOperationException roe) {
					throw new IOException(roe);
				}
			}
			return this;
		}

		/**
		 * Set the job catalog where templates may be found.
		 */
		public Builder jobCatalog(JobCatalog jobCatalog) {
			this.jobCatalog = new PackagedTemplatesJobCatalogDecorator(jobCatalog);
			return this;
		}

		/**
		 * Add a {@link JobResolutionCallbacks} to the resolution process.
		 */
		public Builder jobResolutionAction(JobResolutionCallbacks action) {
			this.jobResolutionCallbacks.add(action);
			return this;
		}

		/**
		 * @return a {@link JobSpecResolver}
		 */
		public JobSpecResolver build() {
			return new JobSpecResolver(this.jobCatalog, this.jobResolutionCallbacks, this.sysConfig);
		}
	}

	/**
	 * Resolve an input {@link JobSpec} applying any templates.
	 *
	 * @throws SpecNotFoundException If no template exists with the specified URI.
	 * @throws JobTemplate.TemplateException If the template throws an exception during resolution.
	 * @throws ConfigException If the job config cannot be resolved.
	 */
	public ResolvedJobSpec resolveJobSpec(JobSpec jobSpec)
			throws SpecNotFoundException, JobTemplate.TemplateException, ConfigException {
		if (jobSpec instanceof ResolvedJobSpec) {
			return (ResolvedJobSpec) jobSpec;
		}

		JobTemplate jobTemplate = null;
		if (jobSpec.getJobTemplate().isPresent()) {
			jobTemplate = jobSpec.getJobTemplate().get();
		} else if (jobSpec.getTemplateURI().isPresent()) {
			jobTemplate = jobCatalog.getTemplate(jobSpec.getTemplateURI().get());
		}

		for (JobResolutionCallbacks action : this.jobResolutionCallbacks) {
			action.beforeResolution(this, jobSpec, jobTemplate);
		}
		Config resolvedConfig;
		if (jobTemplate == null) {
			resolvedConfig = jobSpec.getConfig().resolve();
		} else {
			resolvedConfig = jobTemplate.getResolvedConfig(jobSpec.getConfig()).resolve();
		}
		ResolvedJobSpec resolvedJobSpec = new ResolvedJobSpec(jobSpec, resolvedConfig);
		for (JobResolutionCallbacks action : this.jobResolutionCallbacks) {
			action.afterResolution(this, resolvedJobSpec);
		}

		return resolvedJobSpec;
	}

}
