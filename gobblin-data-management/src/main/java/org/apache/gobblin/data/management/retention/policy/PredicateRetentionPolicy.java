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

package org.apache.gobblin.data.management.retention.policy;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.retention.version.DatasetVersion;


/**
 * Implementation of {@link RetentionPolicy} that marks a {@link DatasetVersion} for deletion if it does not pass a
 * specified {@link Predicate}. The {@link Predicate} class is determined by the key
 * {@link #RETENTION_POLICY_PREDICATE_CLASS}.
 */
public class PredicateRetentionPolicy implements RetentionPolicy<DatasetVersion> {

  private final Predicate<DatasetVersion> predicate;

  private static final String RETENTION_POLICY_PREDICATE_CLASS = "org.apache.gobblin.retention.retention.policy.predicate.class";

  @SuppressWarnings("unchecked")
  public PredicateRetentionPolicy(Properties props) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException,
      NoSuchMethodException {
    this.predicate =
        (Predicate<DatasetVersion>) Class.forName(props.getProperty(RETENTION_POLICY_PREDICATE_CLASS))
            .getConstructor(Properties.class).newInstance(props);
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public Collection<DatasetVersion> listDeletableVersions(List<DatasetVersion> allVersions) {
    return Lists.newArrayList(Iterables.filter(allVersions, Predicates.not(this.predicate)));
  }
}
