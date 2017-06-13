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

package gobblin.data.management.retention.policy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.version.DatasetVersion;


/**
 * Implementation of {@link gobblin.data.management.retention.policy.RetentionPolicy} that allows combining different
 * policies through a union or intersect operation. It will combine the delete sets from each sub-policy using the
 * specified operation.
 *
 * <p>
 * For example, if there are five versions of a dataset, a, b, c, d, e, policy1 would delete versions a, b, while
 * policy2 would delete versions b,c, using {@link CombineRetentionPolicy} will delete versions a, b, c if the
 * operation is UNION, or it will delete only version b if the operation is INTERSECT.
 * </p>
 *
 * <p>
 *   {@link CombineRetentionPolicy} expects the following configurations:
 *   * gobblin.retention.combine.retention.policy.class.* : specifies the classes of the policies to combine. * can be
 *            any value, and each such configuration defines only one class.
 *   * gobblin.retention.combine.retention.policy.delete.sets.combine.operation : operation used to combine delete
 *            sets. Can be UNION or INTERSECT.
 *   Additionally, any configuration necessary for combined policies must be specified.
 * </p>
 */
public class CombineRetentionPolicy<T extends DatasetVersion> implements RetentionPolicy<T> {

  public static final String RETENTION_POLICIES_PREFIX =
      DatasetCleaner.CONFIGURATION_KEY_PREFIX + "combine.retention.policy.class.";
  public static final String DELETE_SETS_COMBINE_OPERATION =
      DatasetCleaner.CONFIGURATION_KEY_PREFIX + "combine.retention.policy.delete.sets.combine.operation";

  public enum DeletableCombineOperation {
    INTERSECT,
    UNION
  }

  private final List<RetentionPolicy<T>> retentionPolicies;
  private final DeletableCombineOperation combineOperation;

  public CombineRetentionPolicy(List<RetentionPolicy<T>> retentionPolicies,
      DeletableCombineOperation combineOperation) {
    this.combineOperation = combineOperation;
    this.retentionPolicies = retentionPolicies;
  }

  @SuppressWarnings("unchecked")
  public CombineRetentionPolicy(Properties props) throws IOException {
    Preconditions.checkArgument(props.containsKey(DELETE_SETS_COMBINE_OPERATION), "Combine operation not specified.");

    ImmutableList.Builder<RetentionPolicy<T>> builder = ImmutableList.builder();

    for (String property : props.stringPropertyNames()) {
      if (property.startsWith(RETENTION_POLICIES_PREFIX)) {

        try {
          builder.add((RetentionPolicy<T>) ConstructorUtils
              .invokeConstructor(Class.forName(props.getProperty(property)), props));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
            | ClassNotFoundException e) {
          throw new IllegalArgumentException(e);
        }
      }
    }

    this.retentionPolicies = builder.build();
    if (this.retentionPolicies.size() == 0) {
      throw new IOException("No retention policies specified for " + CombineRetentionPolicy.class.getCanonicalName());
    }

    this.combineOperation =
        DeletableCombineOperation.valueOf(props.getProperty(DELETE_SETS_COMBINE_OPERATION).toUpperCase());

  }

  /**
   * Returns the most specific common superclass for the {@link #versionClass} of each embedded policy.
   */
  @SuppressWarnings("unchecked")
  @Override
  public Class<T> versionClass() {
    if (this.retentionPolicies.size() == 1) {
      return (Class<T>) this.retentionPolicies.get(0).versionClass();
    }

    Class<T> klazz = (Class<T>) this.retentionPolicies.get(0).versionClass();
    for (RetentionPolicy<T> policy : this.retentionPolicies) {
      klazz = commonSuperclass(klazz, (Class<T>) policy.versionClass());
    }
    return klazz;
  }

  @Override
  public Collection<T> listDeletableVersions(final List<T> allVersions) {

    List<Set<T>> candidateDeletableVersions =
        Lists.newArrayList(Iterables.transform(this.retentionPolicies, new Function<RetentionPolicy<T>, Set<T>>() {
          @SuppressWarnings("deprecation")
          @Nullable
          @Override
          public Set<T> apply(RetentionPolicy<T> input) {
            return Sets.newHashSet(input.listDeletableVersions(allVersions));
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
  public Class<T> commonSuperclass(Class<T> classA, Class<T> classB) {

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
      return (Class<T>) klazz;
    }
    // this should never happen, but there for safety
    return (Class<T>) DatasetVersion.class;
  }

  private Set<T> intersectDatasetVersions(Collection<Set<T>> sets) {
    if (sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<T>> it = sets.iterator();
    Set<T> outputSet = it.next();
    while (it.hasNext()) {
      outputSet = Sets.intersection(outputSet, it.next());
    }
    return outputSet;
  }

  private Set<T> unionDatasetVersions(Collection<Set<T>> sets) {
    if (sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<T>> it = sets.iterator();
    Set<T> outputSet = it.next();
    while (it.hasNext()) {
      outputSet = Sets.union(outputSet, it.next());
    }
    return outputSet;
  }
}
