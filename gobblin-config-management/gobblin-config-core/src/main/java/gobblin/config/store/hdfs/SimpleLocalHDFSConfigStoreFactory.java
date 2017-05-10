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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.typesafe.config.Config;


/**
 * Extension of {@link SimpleHDFSConfigStoreFactory} that creates a {@link SimpleHadoopFilesystemConfigStore} which works for the
 * local file system.
 */
public class SimpleLocalHDFSConfigStoreFactory extends SimpleHadoopFilesystemConfigStoreFactory {

  private static final String LOCAL_HDFS_SCHEME_NAME = "file";

  public SimpleLocalHDFSConfigStoreFactory() {
  }

  public SimpleLocalHDFSConfigStoreFactory(Config factoryConfig) {
    super(factoryConfig);
  }

  @Override
  protected String getPhysicalScheme() {
    return LOCAL_HDFS_SCHEME_NAME;
  }

  @Override
  protected FileSystem getDefaultStoreFs(Config factoryConfig, Optional<URI> configDefinedDefaultURI) {
    try {
      return FileSystem.getLocal(new Configuration());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  protected URI getDefaultRootDir(Config factoryConfig, FileSystem defaultFileSystem,
      Optional<URI> configDefinedDefaultURI) {
    // Return null because lack of authority does not indicate that a default root directory should be used
    return null;
  }

  @Override
  protected boolean isAuthorityRequired() {
    return false;
  }
}
