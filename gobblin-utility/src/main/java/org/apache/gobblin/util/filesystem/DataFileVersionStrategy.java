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

package org.apache.gobblin.util.filesystem;

import com.typesafe.config.Config;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * An interface to set and get "versions" to data files.
 *
 * The version is a rough signature to the data contents. It allows data preserving functionality (like copy) to replicate
 * the version independently of metadata with other semantics like file modification time.
 *
 * Examples where this might be useful is data syncing between two locations. Relying on modification times to detect
 * data changes may lead to a feedback loop of copying: data gets created at location A at time 0,
 * at time 1 data is copied to location B, sync mechanism might incorrectly believe that since mod time of location B
 * is higher, it should be synced back to location A, etc.
 *
 * Required properties:
 * - REPLICABLE: Two calls to `getVersion` on a file that has clearly not been modified must return the same version.
 * - MONOTONOUS: The default version of a file is an increasing function of modification time.
 * - CONSERVATIVE: If file f had its version last set to v, but the versioning implementation determines the file MIGHT
 *                 have been modified and it chooses to return a version, it will return a value strictly larger than v.
 *
 * A common pattern to achieve monotonicity and conservativeness will be to invalidate the version of a data file
 * if it is detected that the file was modified without updating the version (e.g. by a process which is unaware of versioning).
 *
 * @param <T> the type for the version objects. Must be comparable and serializable.
 */
public interface DataFileVersionStrategy<T extends Comparable<T> & Serializable> {

  /**
   * Characteristics a {@link DataFileVersionStrategy} may have.
   */
  enum Characteristic {
    /** The default version for a data file is the modtime of the file. Versions can in general be compared against modtimes. */
    COMPATIBLE_WITH_MODTIME,
    /** Version can be explicitly set. If false, `set*` methods will always return false */
    SETTABLE,
    /** If a file has been modified and a set* method was not called, `getVersion` will throw an error. */
    STRICT
  }

  String DATA_FILE_VERSION_STRATEGY_KEY = "org.apache.gobblin.dataFileVersionStrategy";
  String DEFAULT_DATA_FILE_VERSION_STRATEGY = "modtime";

  /**
   * Instantiate a {@link DataFileVersionStrategy} according to input configuration.
   */
  static DataFileVersionStrategy instantiateDataFileVersionStrategy(FileSystem fs, Config config) throws IOException {
    String versionStrategy = ConfigUtils.getString(config, DATA_FILE_VERSION_STRATEGY_KEY, DEFAULT_DATA_FILE_VERSION_STRATEGY);

    ClassAliasResolver resolver = new ClassAliasResolver(DataFileVersionFactory.class);

    try {
      Class<? extends DataFileVersionFactory> klazz = resolver.resolveClass(versionStrategy);
      return klazz.newInstance().createDataFileVersionStrategy(fs, config);
    } catch (ReflectiveOperationException roe) {
      throw new IOException(roe);
    }
  }

  /**
   * A Factory for {@link DataFileVersionStrategy}s.
   */
  interface DataFileVersionFactory<T extends Comparable<T> & Serializable> {
    /**
     * Build a {@link DataFileVersionStrategy} with the input configuration.
     */
    DataFileVersionStrategy<T> createDataFileVersionStrategy(FileSystem fs, Config config);
  }

  /**
   * Get the version of a path.
   */
  T getVersion(Path path) throws IOException;

  /**
   * Set the version of a path to a specific version (generally replicated from another path).
   *
   * @return false if the version is not settable.
   * @throws IOException if the version is settable but could not be set successfully.
   */
  boolean setVersion(Path path, T version) throws IOException;

  /**
   * Set the version of a path to a value automatically set by the versioning system. Note this call must respect the
   * monotonicity requirement.
   *
   * @return false if the version is not settable.
   * @throws IOException if the version is settable but could not be set successfully.
   */
  boolean setDefaultVersion(Path path) throws IOException;

  /**
   * @return The list of optional characteristics this {@link DataFileVersionStrategy} satisfies.
   */
  Set<Characteristic> applicableCharacteristics();

  /**
   * @return whether this implementation have the specified characteristic.
   */
  default boolean hasCharacteristic(Characteristic characteristic) {
    return applicableCharacteristics().contains(characteristic);
  }

}
