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
import org.apache.hadoop.fs.Path;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import com.google.common.collect.ImmutableMap;
import com.sun.nio.zipfs.ZipFileSystem;


/**
 * Extension of {@link IvyConfigStoreFactory} that creates a {@link IvyConfigStoreFactory} which works for the
 * local file system. Instead of downloading zip from jar artifactory, just use the testing zip files in classpath.
 */
public class SimpleLocalIvyConfigStoreFactory extends IvyConfigStoreFactory {

  private String currentVersion;

  private static ThreadLocal<FileSystem> THREADLOCAL_FS = new ThreadLocal<>();

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

  /**
   *
   * @param configKey whose path contains the physical path to the zip file.
   * @return
   * @throws ConfigStoreCreationException
   */
  @Override
  public ZipFileConfigStore createConfigStore(URI configKey)
      throws ConfigStoreCreationException {
    Properties factoryProps = new Properties();
    for (NameValuePair param : URLEncodedUtils.parse(configKey, "UTF-8")) {
      factoryProps.setProperty(param.getName(), param.getValue());
    }

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
}
