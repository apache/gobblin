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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;

import lombok.Getter;


/**
 * An implementation of {@link HdfsDatasetDescriptor}.
 */
@Alpha
public class BaseHdfsDatasetDescriptor implements HdfsDatasetDescriptor {
  @Getter
  private final String path;
  @Getter
  private final String format;
  @Getter
  private final String description;
  @Getter
  private final String platform;

  public BaseHdfsDatasetDescriptor(Config config) {
    Preconditions.checkArgument(config.hasPath(DatasetDescriptorConfigKeys.PATH_KEY), String.format("Missing required property %s", DatasetDescriptorConfigKeys.PATH_KEY));
    Preconditions.checkArgument(config.hasPath(DatasetDescriptorConfigKeys.FORMAT_KEY), String.format("Missing required property %s", DatasetDescriptorConfigKeys.FORMAT_KEY));

    this.path = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PATH_KEY, null);
    this.format = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.FORMAT_KEY, null);
    this.description = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.DESCRIPTION_KEY, "");
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
    return this.equals(o);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HdfsDatasetDescriptor other = (HdfsDatasetDescriptor) o;
    if(this.getPlatform() == null || other.getPlatform() == null) {
      return false;
    }
    if(!this.getPlatform().equalsIgnoreCase(other.getPlatform()) || !(o instanceof HdfsDatasetDescriptor)) {
      return false;
    }

    return this.getPath().equals(other.getPath()) && this.getFormat().equalsIgnoreCase(other.getFormat());
  }

  @Override
  public String toString() {
     return "(" + Joiner.on(",").join(this.getPlatform(),this.getPath(),this.getFormat()) + ")";
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }
}
