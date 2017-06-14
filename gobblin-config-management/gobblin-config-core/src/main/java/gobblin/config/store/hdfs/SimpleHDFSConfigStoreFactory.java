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
package gobblin.config.store.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.ConfigStoreFactory;
import gobblin.util.ConfigUtils;


/**
 * An implementation of {@link ConfigStoreFactory} for creating {@link SimpleHadoopFilesystemConfigStore}s. This class only works
 * the physical scheme {@link #HDFS_SCHEME_NAME}.
 *
 * @see SimpleHadoopFilesystemConfigStore
 */
public class SimpleHDFSConfigStoreFactory extends SimpleHadoopFilesystemConfigStoreFactory {

  protected static final String HDFS_SCHEME_NAME = "hdfs";

  /** Instantiates a new instance using standard typesafe config defaults:
   * {@link ConfigFactory#load()} */
  public SimpleHDFSConfigStoreFactory() {
    this(ConfigUtils.getConfigOrEmpty(ConfigFactory.load(), DEFAULT_CONFIG_NAMESPACE));
  }

  /**
   * Instantiates a new instance of the factory with the specified config. The configuration is
   * expected to be scoped, i.e. the properties should not be prefixed.
   */
  public SimpleHDFSConfigStoreFactory(Config factoryConfig) {
    super(factoryConfig);
  }

  @Override
  protected FileSystem getDefaultStoreFs(Config factoryConfig, Optional<URI> configDefinedDefaultURI) {
    try {
      if (configDefinedDefaultURI.isPresent() && configDefinedDefaultURI.get().getAuthority() != null) {
        return FileSystem.get(configDefinedDefaultURI.get(), new Configuration());
      } else {
        FileSystem fs = FileSystem.get(new Configuration());
        return HDFS_SCHEME_NAME.equals(fs.getScheme()) ? fs : null;
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Could not create default store fs for scheme " + getScheme());
    }
  }

  @Override
  protected URI getDefaultRootDir(Config factoryConfig, FileSystem defaultFileSystem,
      Optional<URI> configDefinedDefaultURI) {
    return configDefinedDefaultURI.or(defaultFileSystem.getHomeDirectory().toUri());
  }

  @Override
  protected String getPhysicalScheme() {
    return HDFS_SCHEME_NAME;
  }
}
