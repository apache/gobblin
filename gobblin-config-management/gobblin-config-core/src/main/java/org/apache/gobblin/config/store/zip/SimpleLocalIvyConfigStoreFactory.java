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

package org.apache.gobblin.config.store.zip;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Properties;

import org.apache.gobblin.config.store.api.ConfigStoreCreationException;
import org.apache.gobblin.config.store.api.ConfigStoreFactory;
import org.apache.hadoop.fs.Path;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import com.google.common.collect.ImmutableMap;
import com.sun.nio.zipfs.ZipFileSystem;


/**
 * An implementation of {@link ConfigStoreFactory} that takes a locally-existed zip as the backend of Config-store
 * and creates a {@link ZipFileConfigStore} with it.
 *
 * {@link ZipFileConfigStore} has more advantage on encapsulating Config-store itself comparing to
 * {@link org.apache.gobblin.config.store.hdfs.SimpleHadoopFilesystemConfigStore}, where the latter could, for example,
 * cause small-file problem on HDFS as the size of Config-Store grows.
 */
public class SimpleLocalIvyConfigStoreFactory implements ConfigStoreFactory<ZipFileConfigStore> {

  private String currentVersion;

  private static ThreadLocal<FileSystem> THREADLOCAL_FS = new ThreadLocal<>();

  static final String STORE_PREFIX_KEY = "storePrefix";
  static final String IVY_SCHEME_PREFIX = "ivy-";

  /**
   * If not specified an version, assigned with an default version since the primary usage of this class
   * is for testing.
   */
  public SimpleLocalIvyConfigStoreFactory() {
    this.currentVersion = "v1.0";
  }

  public SimpleLocalIvyConfigStoreFactory(String configStoreVersion) {
    this.currentVersion = configStoreVersion;
  }

  @Override
  public String getScheme() {
    return getSchemePrefix() + "file";
  }

  protected String getSchemePrefix() {
    return IVY_SCHEME_PREFIX;
  }

  /**
   *
   * @param configKey whose path contains the physical path to the zip file.
   * @return
   * @throws ConfigStoreCreationException
   */
  @Override
  public ZipFileConfigStore createConfigStore(URI configKey)
      throws ConfigStoreCreationException {
    Properties factoryProps = parseUriIntoParameterSet(configKey);

    try {
      // Construct URI as jar for zip file, as "jar" is the scheme for ZipFs.
      URI uri = new URI("jar:file", null, new Path(factoryProps.getProperty("storePath")).toString(), null);

      /** Using threadLocal to avoid {@link java.nio.file.FileSystemAlreadyExistsException} */
      if (THREADLOCAL_FS.get() == null) {
        FileSystem zipFs = FileSystems.newFileSystem(uri, ImmutableMap.of());
        THREADLOCAL_FS.set(zipFs);
      }

      return new ZipFileConfigStore((ZipFileSystem) THREADLOCAL_FS.get(), getBaseURI(configKey), currentVersion,
          factoryProps.getProperty(STORE_PREFIX_KEY, ""));
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException("Unable to load zip from classpath. ", e);
    }
  }

  /**
   * Parse the configKey and obtain the parameters set required to ivy coordinates.
   */
  Properties parseUriIntoParameterSet(URI configKey) {
    Properties factoryProps = new Properties();
    for (NameValuePair param : URLEncodedUtils.parse(configKey, "UTF-8")) {
      factoryProps.setProperty(param.getName(), param.getValue());
    }
    return factoryProps;
  }

  /**
   * Base URI for a config store should be root of the zip file, so change path part of URI to be null
   */
  URI getBaseURI(URI configKey) throws URISyntaxException {
    return new URI(configKey.getScheme(), configKey.getAuthority(), "/", configKey.getQuery(), configKey.getFragment());
  }
}
