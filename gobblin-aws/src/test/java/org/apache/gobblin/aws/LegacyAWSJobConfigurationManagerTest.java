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

package org.apache.gobblin.aws;

import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


/**
 * Unit tests for {@link AWSJobConfigurationManager}.
 *
 * @author Abhishek Tiwari
 */
@Test(groups = { "gobblin.aws" })
public class LegacyAWSJobConfigurationManagerTest extends BaseAWSJobConfigurationManagerTest {
  @Override
  protected Config getConfig(String jobConfZipUri) {
    return ConfigFactory.empty()
            .withValue(GobblinAWSConfigurationKeys.JOB_CONF_S3_URI_KEY, ConfigValueFactory.fromAnyRef(jobConfZipUri));
  }
}
