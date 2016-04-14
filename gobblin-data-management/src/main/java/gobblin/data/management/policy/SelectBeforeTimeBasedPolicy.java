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

import com.google.common.base.Predicate;
import com.typesafe.config.Config;

import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * An implementation of {@link AbstractTimeBasedSelectionPolicy} which returns versions that are older than a certain time.
 */
public class SelectBeforeTimeBasedPolicy extends AbstractTimeBasedSelectionPolicy {

  public SelectBeforeTimeBasedPolicy(Config conf) {
    super(conf);
  }

  public SelectBeforeTimeBasedPolicy(Properties props) {
    super(props);
  }

  @Override
  protected Predicate<TimestampedDatasetVersion> getSelectionPredicate() {
    return new Predicate<TimestampedDatasetVersion>() {
      @Override
      public boolean apply(TimestampedDatasetVersion version) {
        return version.getDateTime().plus(lookBackPeriod).isBeforeNow();
      }
    };
  }
}
