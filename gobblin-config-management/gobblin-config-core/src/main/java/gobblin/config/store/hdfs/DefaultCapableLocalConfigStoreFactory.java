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

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.typesafe.config.Config;


/**
 * A {@link SimpleHDFSConfigStoreFactory} that uses the user directory as the config store location. Use scheme
 * "default-file".
 */
public class DefaultCapableLocalConfigStoreFactory extends SimpleLocalHDFSConfigStoreFactory {

  public DefaultCapableLocalConfigStoreFactory() {
  }

  public DefaultCapableLocalConfigStoreFactory(Config factoryConfig) {
    super(factoryConfig);
  }

  public static final String SCHEME_PREFIX = "default-";

  @Override
  protected URI getDefaultRootDir(Config factoryConfig, FileSystem defaultFileSystem,
      Optional<URI> configDefinedDefaultURI) {
    try {
      if (configDefinedDefaultURI.isPresent()) {
        return configDefinedDefaultURI.get();
      } else {
        return new URI(System.getProperty("user.dir"));
      }
    } catch (URISyntaxException use) {
      throw new RuntimeException(use);
    }
  }

  @Override
  protected String getSchemePrefix() {
    return SCHEME_PREFIX;
  }
}
