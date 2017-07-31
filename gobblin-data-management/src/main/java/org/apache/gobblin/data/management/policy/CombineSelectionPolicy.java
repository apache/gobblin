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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import lombok.ToString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.retention.policy.CombineRetentionPolicy;
import org.apache.gobblin.data.management.version.DatasetVersion;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Implementation of {@link org.apache.gobblin.data.management.policy.VersionSelectionPolicy} that allows combining different
 * policies through a union or intersect operation. It will combine the selected sets from each sub-policy using the
 * specified operation.
 *
 * <p>
 * For example, if there are five versions of a dataset, a, b, c, d, e, policy1 would select versions a, b, while
 * policy2 would select versions b,c, using {@link CombineSelectionPolicy} will select versions a, b, c if the
 * operation is UNION, or it will select only version b if the operation is INTERSECT.
 * </p>
 *
 * <p>
 *   {@link CombineRetentionPolicy} expects the following configurations:
 *   * version.selection.policy.class.* : specifies the classes of the policies to combine. * can be
 *            any value, and each such configuration defines only one class.
 *   * version.selection.combine.operation : operation used to combine delete
 *            sets. Can be UNION or INTERSECT.
 *   Additionally, any configuration necessary for combined policies must be specified.
 * </p>
 */
@ToString
public class CombineSelectionPolicy implements VersionSelectionPolicy<DatasetVersion> {

  public static final String VERSION_SELECTION_POLICIES_PREFIX = "selection.combine.policy.classes";
  public static final String VERSION_SELECTION_COMBINE_OPERATION = "selection.combine.operation";

  public enum CombineOperation {
    INTERSECT,
    UNION
  }

  private final List<VersionSelectionPolicy<DatasetVersion>> selectionPolicies;
  private final CombineOperation combineOperation;

  public CombineSelectionPolicy(List<VersionSelectionPolicy<DatasetVersion>> selectionPolicies,
      CombineOperation combineOperation) {
    this.combineOperation = combineOperation;
    this.selectionPolicies = selectionPolicies;
  }

  @SuppressWarnings("unchecked")
  public CombineSelectionPolicy(Config config, Properties jobProps) throws IOException {
    Preconditions.checkArgument(config.hasPath(VERSION_SELECTION_POLICIES_PREFIX), "Combine operation not specified.");

    ImmutableList.Builder<VersionSelectionPolicy<DatasetVersion>> builder = ImmutableList.builder();

    for (String combineClassName : config.getStringList(VERSION_SELECTION_POLICIES_PREFIX)) {
      try {
        builder.add((VersionSelectionPolicy<DatasetVersion>) GobblinConstructorUtils.invokeFirstConstructor(
            Class.forName(combineClassName), ImmutableList.<Object> of(config), ImmutableList.<Object> of(jobProps)));
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
          | ClassNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    }

    this.selectionPolicies = builder.build();
    if (this.selectionPolicies.size() == 0) {
      throw new IOException("No selection policies specified for " + CombineSelectionPolicy.class.getCanonicalName());
    }

    this.combineOperation =
        CombineOperation.valueOf(config.getString(VERSION_SELECTION_COMBINE_OPERATION).toUpperCase());
  }

  public CombineSelectionPolicy(Properties props) throws IOException {
    this(ConfigFactory.parseProperties(props), props);

  }

  /**
   * Returns the most specific common superclass for the {@link #versionClass} of each embedded policy.
   */
  @Override
  public Class<? extends DatasetVersion> versionClass() {
    if (this.selectionPolicies.size() == 1) {
      return this.selectionPolicies.get(0).versionClass();
    }

    Class<? extends DatasetVersion> klazz = this.selectionPolicies.get(0).versionClass();
    for (VersionSelectionPolicy<? extends DatasetVersion> policy : this.selectionPolicies) {
      klazz = commonSuperclass(klazz, policy.versionClass());
    }
    return klazz;
  }

  @Override
  public Collection<DatasetVersion> listSelectedVersions(final List<DatasetVersion> allVersions) {

    List<Set<DatasetVersion>> candidateDeletableVersions = Lists.newArrayList(Iterables
        .transform(this.selectionPolicies, new Function<VersionSelectionPolicy<DatasetVersion>, Set<DatasetVersion>>() {
          @Nullable
          @Override
          public Set<DatasetVersion> apply(VersionSelectionPolicy<DatasetVersion> input) {
            return Sets.newHashSet(input.listSelectedVersions(allVersions));
          }
        }));

    switch (this.combineOperation) {
      case INTERSECT:
        return intersectDatasetVersions(candidateDeletableVersions);
      case UNION:
        return unionDatasetVersions(candidateDeletableVersions);
      default:
        throw new RuntimeException("Combine operation " + this.combineOperation + " not recognized.");
    }

  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  public static Class<? extends DatasetVersion> commonSuperclass(Class<? extends DatasetVersion> classA,
      Class<? extends DatasetVersion> classB) {

    if (classA.isAssignableFrom(classB)) {
      // a is superclass of b, so return class of a
      return classA;
    }
    // a is not superclass of b. Either b is superclass of a, or they are not in same branch
    // find closest superclass of a that is also a superclass of b
    Class<?> klazz = classA;
    while (!klazz.isAssignableFrom(classB)) {
      klazz = klazz.getSuperclass();
    }
    if (DatasetVersion.class.isAssignableFrom(klazz)) {
      return (Class<? extends DatasetVersion>) klazz;
    }
    // this should never happen, but there for safety
    return DatasetVersion.class;
  }

  private static Set<DatasetVersion> intersectDatasetVersions(Collection<Set<DatasetVersion>> sets) {
    if (sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<DatasetVersion>> it = sets.iterator();
    Set<DatasetVersion> outputSet = it.next();
    while (it.hasNext()) {
      outputSet = Sets.intersection(outputSet, it.next());
    }
    return outputSet;
  }

  private static Set<DatasetVersion> unionDatasetVersions(Collection<Set<DatasetVersion>> sets) {
    if (sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<DatasetVersion>> it = sets.iterator();
    Set<DatasetVersion> outputSet = it.next();
    while (it.hasNext()) {
      outputSet = Sets.union(outputSet, it.next());
    }
    return outputSet;
  }
}
