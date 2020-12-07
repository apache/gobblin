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

package org.apache.gobblin.service.modules.dataset;

import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;


/**
 * The interface for dataset descriptors. Each dataset is described in terms of the following attributes:
 *  <ul>
 *    <p> platform (e.g. HDFS, ADLS, JDBC). </p>
 *    <p> path, which describes the fully qualified name of the dataset. </p>
 *    <p> a format descriptor, which encapsulates its representation (e.g. avro, csv), codec (e.g. gzip, deflate), and
 *    encryption config (e.g. aes_rotating, gpg). </p>
 *  </ul>
 */
@Alpha
public interface DatasetDescriptor {
  /**
   * @return the dataset platform i.e. the storage system backing the dataset (e.g. HDFS, ADLS, JDBC etc.)
   */
  public String getPlatform();

  /**
   * Returns the fully qualified name of a dataset. The fully qualified name is the absolute directory path of a dataset
   * when the dataset is backed by a FileSystem. In the case of a database table, it is dbName.tableName.
   * @return dataset path.
   */
  public String getPath();

  /**
   *
   * @return storage format of the dataset.
   */
  public FormatConfig getFormatConfig();

  /**
   * @return true if retention has been applied to the dataset.
   */
  public boolean isRetentionApplied();

  /**
   * @return a human-readable description of the dataset.
   */
  public String getDescription();

  /**
   * @return true if this {@link DatasetDescriptor} contains the other {@link DatasetDescriptor} i.e. the
   * datasets described by the other {@link DatasetDescriptor} is a subset of this {@link DatasetDescriptor}.
   * This operation is non-commutative.
   */
  public boolean contains(DatasetDescriptor other);

  /**
   * @return the raw config.
   */
  public Config getRawConfig();
}
