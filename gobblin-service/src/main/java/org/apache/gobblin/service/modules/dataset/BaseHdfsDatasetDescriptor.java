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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigResolveOptions;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An implementation of {@link HdfsDatasetDescriptor}.
 */
@Alpha
public class BaseHdfsDatasetDescriptor implements HdfsDatasetDescriptor {
  public static final String HDFS_PLATFORM_NAME = "hdfs";

  private final String path;
  private final String datasetUrn;
  private final String format;
  private final String description;

  public BaseHdfsDatasetDescriptor(Config config) {
    Preconditions.checkArgument(config.hasPath(ServiceConfigKeys.PATH_KEY), String.format("Missing required property %s", ServiceConfigKeys.PATH_KEY));
    Preconditions.checkArgument(config.hasPath(ServiceConfigKeys.FORMAT_KEY), String.format("Missing required property %s", ServiceConfigKeys.FORMAT_KEY));

    this.path = ConfigUtils.getString(config, ServiceConfigKeys.PATH_KEY, null);
    this.datasetUrn = ConfigUtils.getString(config, ServiceConfigKeys.DATASET_URN_KEY, "");
    this.format = ConfigUtils.getString(config, ServiceConfigKeys.FORMAT_KEY, null);
    this.description = ConfigUtils.getString(config, ServiceConfigKeys.DESCRIPTION_KEY, "");
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public String getFormat() {
    return format;
  }

  @Override
  public String getPlatform() {
    return HDFS_PLATFORM_NAME;
  }

  @Override
  public String getType() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getDatasetUrn() {
    return datasetUrn;
  }

  /**
   * A {@link HdfsDatasetDescriptor} is compatible with another {@link DatasetDescriptor} iff they have identical
   * platform, type, path, and format.
   * @return true if this {@link HdfsDatasetDescriptor} is compatibe with another {@link DatasetDescriptor}.
   */
  @Override
  public boolean isCompatibleWith(DatasetDescriptor o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if(!this.getPlatform().equalsIgnoreCase(o.getPlatform()) || !this.getType().equalsIgnoreCase(o.getType())) {
      return false;
    }

    HdfsDatasetDescriptor other = (HdfsDatasetDescriptor) o;
    return this.getPath().equals(other.getPath()) && this.getFormat().equalsIgnoreCase(other.getFormat());
  }
}
