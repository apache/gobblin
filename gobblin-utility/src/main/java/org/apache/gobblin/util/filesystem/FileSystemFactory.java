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

package org.apache.gobblin.util.filesystem;

import java.io.IOException;
import java.net.URI;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link SharedResourceFactory} for creating {@link FileSystem}s.
 *
 * The factory creates a {@link FileSystem} with the correct scheme and applies any {@link FileSystemInstrumentation}
 * found in the classpath.
 */
@Slf4j
public class FileSystemFactory<S extends ScopeType<S>> implements SharedResourceFactory<FileSystem, FileSystemKey, S> {

  public static final String FACTORY_NAME = "filesystem";

  /**
   * Equivalent to {@link FileSystem#get(Configuration)}, but uses the input {@link SharedResourcesBroker} to configure
   * add-ons to the {@link FileSystem} (e.g. throttling, instrumentation).
   */
  public static <S extends ScopeType<S>> FileSystem get(Configuration configuration, SharedResourcesBroker<S> broker)
      throws IOException {
    return get(FileSystem.getDefaultUri(configuration), configuration, broker);
  }

  /**
   * Equivalent to {@link FileSystem#get(URI, Configuration)}, but uses the input {@link SharedResourcesBroker} to configure
   * add-ons to the {@link FileSystem} (e.g. throttling, instrumentation).
   */
  public static <S extends ScopeType<S>> FileSystem get(URI uri, Configuration configuration, SharedResourcesBroker<S> broker)
    throws IOException {
    try {
      return broker.getSharedResource(new FileSystemFactory<S>(), new FileSystemKey(uri, configuration));
    } catch (NotConfiguredException nce) {
      throw new IOException(nce);
    }
  }

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<FileSystem> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, FileSystemKey> config) throws NotConfiguredException {
    try {

      FileSystemKey key = config.getKey();
      URI uri = key.getUri();
      Configuration hadoopConf = key.getConfiguration();

      log.info("Creating instrumented FileSystem for uri " + uri);

      Class<? extends FileSystem> fsClass = FileSystem.getFileSystemClass(uri.getScheme(), hadoopConf);
      if (InstrumentedFileSystem.class.isAssignableFrom(fsClass)) {
        InstrumentedFileSystem tmpfs = (InstrumentedFileSystem) fsClass.newInstance();
        hadoopConf = new Configuration(hadoopConf);
        String schemeKey = "fs." + uri.getScheme() + ".impl";
        hadoopConf.set(schemeKey, tmpfs.underlyingFs.getClass().getName());
      }

      FileSystem fs = FileSystem.newInstance(uri, hadoopConf);
      ServiceLoader<FileSystemInstrumentationFactory> loader = ServiceLoader.load(FileSystemInstrumentationFactory.class);

      for (FileSystemInstrumentationFactory instrumentationFactory : loader) {
        fs = instrumentationFactory.instrumentFileSystem(fs, broker, config);
      }

      return new ResourceInstance<>(fs);
    } catch (IOException | ReflectiveOperationException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, FileSystemKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
