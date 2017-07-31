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

package org.apache.gobblin.data.management.retention.policy.predicates;

import java.util.Properties;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import org.apache.gobblin.data.management.retention.DatasetCleaner;
import org.apache.gobblin.data.management.retention.version.StringDatasetVersion;


/**
 * {@link com.google.common.base.Predicate} for {@link org.apache.gobblin.data.management.retention.policy.PredicateRetentionPolicy}
 * that passes versions matching a user supplied regular expression. (i.e. versions matching the regex will not be
 * deleted).
 */
public class WhitelistPredicate implements Predicate<StringDatasetVersion> {

  public static final String WHITELIST_PATTERN_KEY =
      DatasetCleaner.CONFIGURATION_KEY_PREFIX + "retention.whitelist.pattern";

  private final Pattern whitelist;

  public WhitelistPredicate(Properties properties) {
    Preconditions.checkArgument(properties.containsKey(WHITELIST_PATTERN_KEY));
    this.whitelist = Pattern.compile(properties.getProperty(WHITELIST_PATTERN_KEY));
  }

  @Override
  public boolean apply(StringDatasetVersion input) {
    return this.whitelist.matcher(input.getVersion()).find();
  }

}
