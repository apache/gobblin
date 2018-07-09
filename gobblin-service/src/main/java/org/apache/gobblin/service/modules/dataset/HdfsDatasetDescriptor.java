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

import lombok.Getter;


public class HdfsDatasetDescriptor extends BaseFsDatasetDescriptor {
  @Getter
  private final String platform;

  public HdfsDatasetDescriptor(Config config) {
    super(config);
    this.platform = "hdfs";
  }

  /**
   * A {@link HdfsDatasetDescriptor} is compatible with another {@link DatasetDescriptor} iff they have identical
   * platform, type, path, and format.
   * TODO: Currently isCompatibleWith() only checks if HDFS paths described by the two {@link DatasetDescriptor}s
   * being compared are identical. Need to enhance this for the case of where paths can contain glob patterns.
   * e.g. paths described by the pattern /data/input/* are a subset of paths described by /data/* and hence, the
   * two descriptors should be compatible.
   * @return true if this {@link HdfsDatasetDescriptor} is compatibe with another {@link DatasetDescriptor}.
   */
  @Override
  public boolean isCompatibleWith(DatasetDescriptor o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HdfsDatasetDescriptor)) {
      return false;
    }
    HdfsDatasetDescriptor other = (HdfsDatasetDescriptor) o;
    if (this.getPlatform() == null || other.getPlatform() == null || !this.getPlatform().equalsIgnoreCase(other.getPlatform())) {
      return false;
    }
    return isPropertyCompatibleWith(other) && isPathCompatible(other.getPath());
  }

  /**
   *
   * @param o
   * @return true iff  "this" dataset descriptor is compatible with the "other" and the "other" dataset descriptor is
   * compatible with this dataset descriptor.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HdfsDatasetDescriptor)) {
      return false;
    }
    HdfsDatasetDescriptor other = (HdfsDatasetDescriptor) o;
    if (this.getPlatform() == null || other.getPlatform() == null || !this.getPlatform().equalsIgnoreCase(other.getPlatform())) {
      return false;
    }
    return this.getPath().equals(other.getPath()) && this.getFormat().equalsIgnoreCase(other.getFormat())
        && this.getCodecType().equalsIgnoreCase(other.getCodecType()) && this.getEncryptionConfig().equals(other.getEncryptionConfig());
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }
}
