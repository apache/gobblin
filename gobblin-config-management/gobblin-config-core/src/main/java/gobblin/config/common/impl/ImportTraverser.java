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

package gobblin.config.common.impl;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import com.google.common.cache.Cache;

import gobblin.config.store.api.ConfigKeyPath;

import javax.annotation.concurrent.NotThreadSafe;
import lombok.RequiredArgsConstructor;


/**
 * This class computes a traversal of a graph. Starting at the provided node, it uses the {@link #traversalFunction}
 * to generate a DFS traversal of the graph. The traversal is guaranteed to contain each node at most once. If a cycle
 * is detected during traversal, a {@link CircularDependencyException} will be thrown.
 */
@RequiredArgsConstructor
@NotThreadSafe
class ImportTraverser {
  private final Set<ConfigKeyPath> nodePath = new HashSet<>();
  /** The function returning the ordered neighbors for the input node. */
  private final Function<ConfigKeyPath, List<ConfigKeyPath>> traversalFunction;
  /** A cache used for storing traversals at various nodes. */
  private final Cache<ConfigKeyPath, LinkedList<ConfigKeyPath>> traversalCache;

  /**
   * Traverse the graph starting at the provided node.
   * @param node starting node.
   * @return a List containing the DFS traversal starting at the node.
   * @throws ExecutionException if a cycle is found (the cause will be a {@link CircularDependencyException}) or
   *                  if {@link #traversalFunction} throws an exception.
   */
  public List<ConfigKeyPath> traverseGraphRecursively(ConfigKeyPath node)
      throws ExecutionException {
    return this.traversalCache.get(node, () -> computeRecursiveTraversal(node));
  }

  private LinkedList<ConfigKeyPath> computeRecursiveTraversal(ConfigKeyPath node) {
    try {

      LinkedList<ConfigKeyPath> imports = new LinkedList<>();
      Set<ConfigKeyPath> alreadyIncludedImports = new HashSet<>();

      for (ConfigKeyPath neighbor : this.traversalFunction.apply(node)) {
        addSubtraversal(neighbor, imports, alreadyIncludedImports);
      }

      return imports;
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  private void addSubtraversal(ConfigKeyPath node, LinkedList<ConfigKeyPath> imports, Set<ConfigKeyPath> alreadyIncludedImports)
      throws ExecutionException {

    if (nodePath.contains(node)) {
      throw new CircularDependencyException(node + " is part of a cycle.");
    }

    if (addNodeIfNotAlreadyIncluded(node, imports, alreadyIncludedImports)) {
      nodePath.add(node);
      for (ConfigKeyPath inheritedFromParent : traverseGraphRecursively(node)) {
        addNodeIfNotAlreadyIncluded(inheritedFromParent, imports, alreadyIncludedImports);
      }
      nodePath.remove(node);
    }
  }

  private boolean addNodeIfNotAlreadyIncluded(ConfigKeyPath thisImport,
      LinkedList<ConfigKeyPath> imports, Set<ConfigKeyPath> alreadyIncludedImports) {
    if (alreadyIncludedImports.contains(thisImport)) {
      return false;
    }
    imports.add(thisImport);
    alreadyIncludedImports.add(thisImport);
    return true;
  }

}
