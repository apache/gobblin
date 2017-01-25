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

package gobblin.data.management.policy;

import java.util.Collection;
import java.util.List;

import gobblin.data.management.version.DatasetVersion;


/**
 * Selection policy around versions of a dataset. Specifies which versions of a dataset will be selected.
 */
public interface VersionSelectionPolicy<T extends DatasetVersion> {
  /**
   * Should return class of T.
   * @return class of T.
   */
  public Class<? extends DatasetVersion> versionClass();

  /**
   * Logic to decide which dataset versions will be selected.
   *
   * @param allVersions List of all dataset versions in the file system, sorted from newest to oldest.
   * @return Collection of dataset versions that are selected.
   */
  public Collection<T> listSelectedVersions(List<T> allVersions);
}
