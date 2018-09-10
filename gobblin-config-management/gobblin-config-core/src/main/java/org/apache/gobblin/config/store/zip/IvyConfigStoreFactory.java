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
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import com.sun.nio.zipfs.ZipFileSystem;

import org.apache.gobblin.config.store.api.ConfigStoreCreationException;
import org.apache.gobblin.config.store.api.ConfigStoreFactory;
import org.apache.gobblin.config.store.hdfs.SimpleHDFSConfigStoreFactory;
import org.apache.gobblin.config.store.hdfs.SimpleHDFSStoreMetadata;
import org.apache.gobblin.config.store.hdfs.SimpleHadoopFilesystemConfigStore;
import org.apache.gobblin.util.DownloadUtils;


/**
 * {@link ConfigStoreFactory} that downloads a jar file containing the config store paths through ivy and creates a
 * {@link ZipFileConfigStore} with it. May be useful to avoid making many HDFS calls for large config stores.
 *
 * An ivy settings file must be present on the classpath named {@link DownloadUtils#IVY_SETTINGS_FILE_NAME}
 */
public class IvyConfigStoreFactory implements ConfigStoreFactory<ZipFileConfigStore> {

  private static final String IVY_SCHEME_PREFIX = "ivy-";
  private static final String ORG_KEY = "org";
  private static final String MODULE_KEY = "module";
  private static final String STORE_PREFIX_KEY = "storePrefix";

  @Override
  public String getScheme() {
    return getSchemePrefix() + SimpleHDFSConfigStoreFactory.HDFS_SCHEME_NAME;
  }

  public String getSchemePrefix() {
    return IVY_SCHEME_PREFIX;
  }

  /**
   * Example configKey URI (configuration is passed as part of the query)
   *
   * ivy-hdfs://<hdfsURI>/path/to/config/store?org=<jarOrg>&module=<jarModule>&storePrefix=_CONFIG_STORE
   *
   * ivy-hdfs: scheme for this factory
   * hdfsURI/path/to/config/store: location of HDFS config store (used for getting current version)
   * org/module: org and module of jar containing config store
   * storePrefix: prefix to paths in config store
   */
  @Override
  public ZipFileConfigStore createConfigStore(URI configKey) throws ConfigStoreCreationException {
    if (!configKey.getScheme().equals(getScheme())) {
      throw new ConfigStoreCreationException(configKey, "Config key URI must have scheme " + getScheme());
    }

    Properties factoryProps = new Properties();
    for (NameValuePair param : URLEncodedUtils.parse(configKey, "UTF-8")) {
      factoryProps.setProperty(param.getName(), param.getValue());
    }

    String jarOrg = factoryProps.getProperty(ORG_KEY);
    String jarModule = factoryProps.getProperty(MODULE_KEY);

    if (jarOrg == null || jarModule == null) {
      throw new ConfigStoreCreationException(configKey, "Config key URI must contain org and module to download from");
    }

    try {
      SimpleHDFSStoreMetadata metadata = new SimpleHDFSStoreMetadata(
          org.apache.hadoop.fs.FileSystem.get(new Configuration()), new Path(configKey.getPath(),
          SimpleHadoopFilesystemConfigStore.CONFIG_STORE_NAME));
      String currentVersion = metadata.getCurrentVersion();

      URI[] uris = DownloadUtils.downloadJar(jarOrg, jarModule, currentVersion, false);

      if (uris.length != 1) {
        throw new ConfigStoreCreationException(configKey, "Expected one jar file from URI");
      }

      FileSystem zipFs = FileSystems.newFileSystem(Paths.get(uris[0].getPath()), null);

      if (!(zipFs instanceof ZipFileSystem)) {
        throw new ConfigStoreCreationException(configKey, "Downloaded file must be a zip or jar file");
      }

      return new ZipFileConfigStore((ZipFileSystem) zipFs, configKey, currentVersion, factoryProps.getProperty(STORE_PREFIX_KEY, ""));
    } catch (IOException e) {
      throw new ConfigStoreCreationException(configKey, e);
    }
  }
}

