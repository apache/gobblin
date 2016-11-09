/*
* Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use
* this file except in compliance with the License. You may obtain a copy of the
* License at  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied.
*/
package gobblin.data.management.policy;

import java.util.Properties;

import lombok.ToString;

import org.joda.time.Period;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.data.management.version.TimestampedDatasetVersion;
import gobblin.util.ConfigUtils;


/**
 * Selects {@link TimestampedDatasetVersion}s older than lookbackTime.
 */
@Alias("SelectBeforeTimeBasedPolicy")
@ToString(callSuper=true)
public class SelectBeforeTimeBasedPolicy extends SelectBetweenTimeBasedPolicy {

  private static final String TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY = "selection.timeBased.lookbackTime";

  public SelectBeforeTimeBasedPolicy(Config conf) {
    super(Optional.of(getMinLookbackTime(conf)), Optional.<Period>absent());
  }

  public SelectBeforeTimeBasedPolicy(Properties props) {
    super(ConfigUtils.propertiesToConfig(props));
  }

  private static Period getMinLookbackTime(Config conf) {
    Preconditions.checkArgument(conf.hasPath(TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY),
        String.format("Required property %s is not specified", TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY));
    return SelectBetweenTimeBasedPolicy.getLookBackPeriod(conf.getString(TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY));
  }
}
