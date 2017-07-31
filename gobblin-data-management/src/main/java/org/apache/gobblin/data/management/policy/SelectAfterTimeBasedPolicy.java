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
package org.apache.gobblin.data.management.policy;

import java.util.Properties;

import lombok.ToString;

import org.joda.time.Period;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Selects {@link TimestampedDatasetVersion}s newer than lookbackTime.
 */
@Alias("SelectAfterTimeBasedPolicy")
@ToString(callSuper=true)
public class SelectAfterTimeBasedPolicy extends SelectBetweenTimeBasedPolicy {

  public static final String TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY = "selection.timeBased.lookbackTime";

  public SelectAfterTimeBasedPolicy(Config conf) {
    super(Optional.<Period>absent(), Optional.of(getMaxLookbackTime(conf)));
  }

  public SelectAfterTimeBasedPolicy(Properties props) {
    this(ConfigUtils.propertiesToConfig(props));
  }

  private static Period getMaxLookbackTime(Config conf) {
    Preconditions.checkArgument(conf.hasPath(TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY),
        String.format("Required property %s is not specified", TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY));
    return SelectBetweenTimeBasedPolicy.getLookBackPeriod(conf.getString(TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY));
  }
}
