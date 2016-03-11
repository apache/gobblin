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

import gobblin.data.management.version.FileSystemDatasetVersion;


/**
 * Implementation of {@link gobblin.data.management.policy.VersionSelectionPolicy} that allows combining different
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
public class CombineSelectionPolicy implements VersionSelectionPolicy<FileSystemDatasetVersion> {

  public static final String VERSION_SELECTION_POLICIES_PREFIX = "version.selection.policy.class.";
  public static final String VERSION_SELECTION_COMBINE_OPERATION = "version.selection.combine.operation";

  public enum CombineOperation {
    INTERSECT,
    UNION
  }

  private final List<VersionSelectionPolicy<FileSystemDatasetVersion>> selectionPolicies;
  private final CombineOperation combineOperation;

  public CombineSelectionPolicy(List<VersionSelectionPolicy<FileSystemDatasetVersion>> selectionPolicies,
      CombineOperation combineOperation) throws IOException {
    this.combineOperation = combineOperation;
    this.selectionPolicies = selectionPolicies;
  }

  @SuppressWarnings("unchecked")
  public CombineSelectionPolicy(Properties props) throws IOException {

    Preconditions.checkArgument(props.containsKey(VERSION_SELECTION_POLICIES_PREFIX),
        "Combine operation not specified.");

    ImmutableList.Builder<VersionSelectionPolicy<FileSystemDatasetVersion>> builder = ImmutableList.builder();

    for (String property : props.stringPropertyNames()) {
      if (property.startsWith(VERSION_SELECTION_POLICIES_PREFIX)) {

        try {
          builder.add((VersionSelectionPolicy<FileSystemDatasetVersion>) ConstructorUtils.invokeConstructor(
              Class.forName(props.getProperty(property)), props));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
            | ClassNotFoundException e) {
          throw new IllegalArgumentException(e);
        }
      }
    }

    this.selectionPolicies = builder.build();
    if (this.selectionPolicies.size() == 0) {
      throw new IOException("No selection policies specified for " + CombineSelectionPolicy.class.getCanonicalName());
    }

    this.combineOperation =
        CombineOperation.valueOf(props.getProperty(VERSION_SELECTION_COMBINE_OPERATION).toUpperCase());

  }

  /**
   * Returns the most specific common superclass for the {@link #versionClass} of each embedded policy.
   */
  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    if (this.selectionPolicies.size() == 1) {
      return this.selectionPolicies.get(0).versionClass();
    }

    Class<? extends FileSystemDatasetVersion> klazz = this.selectionPolicies.get(0).versionClass();
    for (VersionSelectionPolicy<? extends FileSystemDatasetVersion> policy : selectionPolicies) {
      klazz = commonSuperclass(klazz, policy.versionClass());
    }
    return klazz;
  }

  @Override
  public Collection<FileSystemDatasetVersion> listSelectedVersions(final List<FileSystemDatasetVersion> allVersions) {

    List<Set<FileSystemDatasetVersion>> candidateDeletableVersions =
        Lists.newArrayList(Iterables.transform(this.selectionPolicies,
            new Function<VersionSelectionPolicy<FileSystemDatasetVersion>, Set<FileSystemDatasetVersion>>() {
              @Nullable
              @Override
              public Set<FileSystemDatasetVersion> apply(VersionSelectionPolicy<FileSystemDatasetVersion> input) {
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
  public Class<? extends FileSystemDatasetVersion> commonSuperclass(Class<? extends FileSystemDatasetVersion> classA,
      Class<? extends FileSystemDatasetVersion> classB) {

    if (classA.isAssignableFrom(classB)) {
      // a is superclass of b, so return class of a
      return classA;
    } else {
      // a is not superclass of b. Either b is superclass of a, or they are not in same branch
      // find closest superclass of a that is also a superclass of b
      Class<?> klazz = classA;
      while (!klazz.isAssignableFrom(classB)) {
        klazz = klazz.getSuperclass();
      }
      if (FileSystemDatasetVersion.class.isAssignableFrom(klazz)) {
        return (Class<? extends FileSystemDatasetVersion>) klazz;
      } else {
        // this should never happen, but there for safety
        return FileSystemDatasetVersion.class;
      }
    }
  }

  private Set<FileSystemDatasetVersion> intersectDatasetVersions(Collection<Set<FileSystemDatasetVersion>> sets) {
    if (sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<FileSystemDatasetVersion>> it = sets.iterator();
    Set<FileSystemDatasetVersion> outputSet = it.next();
    while (it.hasNext()) {
      outputSet = Sets.intersection(outputSet, it.next());
    }
    return outputSet;
  }

  private Set<FileSystemDatasetVersion> unionDatasetVersions(Collection<Set<FileSystemDatasetVersion>> sets) {
    if (sets.size() <= 0) {
      return Sets.newHashSet();
    }
    Iterator<Set<FileSystemDatasetVersion>> it = sets.iterator();
    Set<FileSystemDatasetVersion> outputSet = it.next();
    while (it.hasNext()) {
      outputSet = Sets.union(outputSet, it.next());
    }
    return outputSet;
  }
}
