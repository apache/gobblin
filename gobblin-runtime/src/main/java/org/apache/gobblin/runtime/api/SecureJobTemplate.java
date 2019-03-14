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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * A secure template is a {@link JobTemplate} which only allows the user configuration to specify a static set of
 * keys. This allows most of the template to be non overridable to tightly control how a job executes.
 */
public interface SecureJobTemplate extends JobTemplate {

	/**
	 * Filter the user config to only preserve the keys allowed by a secure template.
	 */
	static Config filterUserConfig(SecureJobTemplate template, Config userConfig, Logger logger) {
		if (!template.isSecure()) {
			return userConfig;
		}
		Config survivingConfig = ConfigFactory.empty();
		for (String key : template.overridableProperties()) {
			if (userConfig.hasPath(key)) {
				survivingConfig = survivingConfig.withValue(key, userConfig.getValue(key));
				userConfig = userConfig.withoutPath(key);
			}
		}

		if (!userConfig.isEmpty()) {
			logger.warn(String.format("Secure template %s ignored the following keys because they are not overridable: %s",
					template.getUri().toString(),
					userConfig.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.joining(", "))));
		}

		return survivingConfig;
	}

	/**
	 * @return Whether the template is secure.
	 */
	boolean isSecure();

	/**
	 * @return If the template is secure, the collection of keys that can be overriden by the user.
	 */
	Collection<String> overridableProperties();
}
