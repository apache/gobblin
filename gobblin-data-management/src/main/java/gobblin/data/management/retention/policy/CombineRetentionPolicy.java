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

package gobblin.data.management.retention.policy;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.version.DatasetVersion;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


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
public class CombineRetentionPolicy implements RetentionPolicy<DatasetVersion> {

  public static final String RETENTION_POLICIES_PREFIX = DatasetCleaner.CONFIGURATION_KEY_PREFIX +
      "combine.retention.policy.class.";
  public static final String DELETE_SETS_COMBINE_OPERATION = DatasetCleaner.CONFIGURATION_KEY_PREFIX +
      "combine.retention.policy.delete.sets.combine.operation";

  public enum DeletableCombineOperation {
    INTERSECT, UNION
  }

  private final List<RetentionPolicy<DatasetVersion>> retentionPolicies;
  private final DeletableCombineOperation combineOperation;

  public CombineRetentionPolicy(Properties props) throws IOException {

    Preconditions.checkArgument(props.containsKey(DELETE_SETS_COMBINE_OPERATION), "Combine operation not specified.");

    ImmutableList.Builder<RetentionPolicy<DatasetVersion>> builder = ImmutableList.builder();

    for(String property : props.stringPropertyNames()) {
      if(property.startsWith(RETENTION_POLICIES_PREFIX)) {
        builder.add(instantiateRetentionPolicy(props.getProperty(property), props));
      }
    }

    this.retentionPolicies = builder.build();
    if(this.retentionPolicies.size() == 0) {
      throw new IOException("No retention policies specified for " + CombineRetentionPolicy.class.getCanonicalName());
    }

    this.combineOperation = DeletableCombineOperation.valueOf(props.getProperty(DELETE_SETS_COMBINE_OPERATION).
        toUpperCase());

  }

  /**
   * Returns the most specific common superclass for the {@link #versionClass} of each embedded policy.
   */
  @Override public Class<? extends DatasetVersion> versionClass() {
    if(this.retentionPolicies.size() == 1) {
      return this.retentionPolicies.get(0).versionClass();
    }

    Class<? extends DatasetVersion> klazz = this.retentionPolicies.get(0).versionClass();
    for(RetentionPolicy<? extends DatasetVersion> policy : retentionPolicies) {
      klazz = commonSuperclass(klazz, policy.versionClass());
    }
    return klazz;
  }

  @Override public Collection<DatasetVersion> listDeletableVersions(final List<DatasetVersion> allVersions) {

    List<Set<DatasetVersion>> candidateDeletableVersions = Lists.newArrayList(Iterables.transform(this.retentionPolicies,
        new Function<RetentionPolicy<DatasetVersion>, Set<DatasetVersion>>() {
      @Nullable @Override public Set<DatasetVersion> apply(RetentionPolicy<DatasetVersion> input) {
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
  public Class<? extends DatasetVersion> commonSuperclass(Class<? extends DatasetVersion> classA,
      Class<? extends DatasetVersion> classB) {

    if(classA.isAssignableFrom(classB)) {
      // a is superclass of b, so return class of a
      return classA;
    } else {
      // a is not superclass of b. Either b is superclass of a, or they are not in same branch
      // find closest superclass of a that is also a superclass of b
      Class<?> klazz = classA;
      while(!klazz.isAssignableFrom(classB)) {
        klazz = klazz.getSuperclass();
      }
      if(DatasetVersion.class.isAssignableFrom(klazz)) {
        return (Class<? extends DatasetVersion>) klazz;
      } else {
        // this should never happen, but there for safety
        return DatasetVersion.class;
      }
    }
  }

  private Set<DatasetVersion> intersectDatasetVersions(Collection<Set<DatasetVersion>> sets) {
    if(sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<DatasetVersion>> it = sets.iterator();
    Set<DatasetVersion> outputSet = it.next();
    while(it.hasNext()) {
      outputSet = Sets.intersection(outputSet, it.next());
    }
    return outputSet;
  }

  private Set<DatasetVersion> unionDatasetVersions(Collection<Set<DatasetVersion>> sets) {
    if(sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<DatasetVersion>> it = sets.iterator();
    Set<DatasetVersion> outputSet = it.next();
    while(it.hasNext()) {
      outputSet = Sets.union(outputSet, it.next());
    }
    return outputSet;
  }

  private RetentionPolicy<DatasetVersion> instantiateRetentionPolicy(String className, Properties props)
      throws IOException {
    try {
      Class<?> klazz = Class.forName(className);
      return  (RetentionPolicy) klazz.
          getConstructor(Properties.class).newInstance(props);
    } catch(ClassNotFoundException exception) {
      throw new IOException(exception);
    } catch(NoSuchMethodException exception) {
      throw new IOException(exception);
    } catch(InstantiationException exception) {
      throw new IOException(exception);
    } catch(IllegalAccessException exception) {
      throw new IOException(exception);
    } catch(InvocationTargetException exception) {
      throw new IOException(exception);
    }
  }
}
