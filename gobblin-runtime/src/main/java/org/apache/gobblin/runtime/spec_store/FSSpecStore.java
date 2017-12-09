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

package org.apache.gobblin.runtime.spec_store;

import com.google.common.io.ByteStreams;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecStore;
import org.apache.gobblin.util.PathUtils;


/**
 * The Spec Store for file system to persist the Spec information.
 * Note:
 * 1. This implementation has no support for caching.
 * 2. This implementation does not performs implicit version management.
 *    For implicit version management, please use a wrapper FSSpecStore.
 */
public class FSSpecStore implements SpecStore {

  protected final Logger log;
  protected final Config sysConfig;
  protected final FileSystem fs;
  protected final String fsSpecStoreDir;
  protected final Path fsSpecStoreDirPath;
  protected final SpecSerDe specSerDe;

  public FSSpecStore(GobblinInstanceEnvironment env, SpecSerDe specSerDe)
      throws IOException {
    this(env.getSysConfig().getConfig(), specSerDe, Optional.<Logger>absent());
  }

  public FSSpecStore(Config sysConfig, SpecSerDe specSerDe) throws IOException {
    this(sysConfig, specSerDe, Optional.<Logger>absent());
  }

  public FSSpecStore(GobblinInstanceEnvironment env, SpecSerDe specSerDe, Optional<Logger> log)
      throws IOException {
    this(env.getSysConfig().getConfig(), specSerDe, log);
  }

  public FSSpecStore(Config sysConfig, SpecSerDe specSerDe, Optional<Logger> log)
      throws IOException {
    Preconditions.checkArgument(sysConfig.hasPath(ConfigurationKeys.SPECSTORE_FS_DIR_KEY),
        "FS SpecStore path must be specified.");

    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.sysConfig = sysConfig;
    this.specSerDe = specSerDe;
    this.fsSpecStoreDir = this.sysConfig.getString(ConfigurationKeys.SPECSTORE_FS_DIR_KEY);
    this.fsSpecStoreDirPath = new Path(this.fsSpecStoreDir);
    this.log.info("FSSpecStore directory is: " + this.fsSpecStoreDir);
    try {
      this.fs = this.fsSpecStoreDirPath.getFileSystem(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException("Unable to detect job config directory file system: " + e, e);
    }
    if (!this.fs.exists(this.fsSpecStoreDirPath)) {
      this.log.info("FSSpecStore directory: " + this.fsSpecStoreDir + " did not exist. Creating it.");
      this.fs.mkdirs(this.fsSpecStoreDirPath);
    }
  }

  /**
   * @param specUri path of the spec
   * @return empty string for topology spec, as topolgies do not have a group,
   *         group name for flow spec
   */
  public static String getSpecGroup(Path specUri) {
    return specUri.getParent().getName();
  }

  public static String getSpecName(Path specUri) {
    return Files.getNameWithoutExtension(specUri.getName());
  }

  private Collection<Spec> getAllVersionsOfSpec(Path spec) {
    Collection<Spec> specs = Lists.newArrayList();

    try {
      specs.add(readSpecFromFile(spec));
    } catch (IOException e) {
      log.warn("Spec {} not found.", spec);
    }
    return specs;
  }

  /**
   * Returns all versions of the spec defined by specUri.
   * Currently, multiple versions are not supported, so this should return exactly one spec.
   * @param specUri URI for the {@link Spec} to be retrieved.
   * @return all versions of the spec.
   */
  @Override
  public Collection<Spec> getAllVersionsOfSpec(URI specUri) {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");
    Path specPath = getPathForURI(this.fsSpecStoreDirPath, specUri, FlowSpec.Builder.DEFAULT_VERSION);
    return getAllVersionsOfSpec(specPath);
  }

  @Override
  public boolean exists(URI specUri) throws IOException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");

    Path specPath = getPathForURI(this.fsSpecStoreDirPath, specUri, FlowSpec.Builder.DEFAULT_VERSION);
    return fs.exists(specPath);
  }

  @Override
  public void addSpec(Spec spec) throws IOException {
    Preconditions.checkArgument(null != spec, "Spec should not be null");

    log.info(String.format("Adding Spec with URI: %s in FSSpecStore: %s", spec.getUri(), this.fsSpecStoreDirPath));
    Path specPath = getPathForURI(this.fsSpecStoreDirPath, spec.getUri(), spec.getVersion());
    writeSpecToFile(specPath, spec);
  }

  @Override
  public boolean deleteSpec(Spec spec) throws IOException {
    Preconditions.checkArgument(null != spec, "Spec should not be null");

    return deleteSpec(spec.getUri(), spec.getVersion());
  }

  @Override
  public boolean deleteSpec(URI specUri) throws IOException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");

      return deleteSpec(specUri, FlowSpec.Builder.DEFAULT_VERSION);
  }

  @Override
  public boolean deleteSpec(URI specUri, String version) throws IOException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");
    Preconditions.checkArgument(null != version, "Version should not be null");

    try {
      log.info(String.format("Deleting Spec with URI: %s in FSSpecStore: %s", specUri, this.fsSpecStoreDirPath));
      Path specPath = getPathForURI(this.fsSpecStoreDirPath, specUri, version);
      return fs.delete(specPath, false);
    } catch (IOException e) {
      throw new IOException(String.format("Issue in removing Spec: %s for Version: %s", specUri, version), e);
    }
  }

  @Override
  public Spec updateSpec(Spec spec) throws IOException, SpecNotFoundException {
    addSpec(spec);
    return spec;
  }

  @Override
  public Spec getSpec(URI specUri) throws SpecNotFoundException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");

    Collection<Spec> specs = getAllVersionsOfSpec(specUri);
    Spec highestVersionSpec = null;

    for (Spec spec : specs) {
      if (null == highestVersionSpec) {
        highestVersionSpec = spec;
      } else if (null != spec.getVersion() && spec.getVersion().compareTo(spec.getVersion()) > 0) {
        highestVersionSpec = spec;
      }
    }

    if (null == highestVersionSpec) {
      throw new SpecNotFoundException(specUri);
    }

    return highestVersionSpec;
  }

  @Override
  public Spec getSpec(URI specUri, String version) throws IOException, SpecNotFoundException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");
    Preconditions.checkArgument(null != version, "Version should not be null");

    Path specPath = getPathForURI(this.fsSpecStoreDirPath, specUri, version);

    if (!fs.exists(specPath)) {
      throw new SpecNotFoundException(specUri);
    }

    return readSpecFromFile(specPath);
  }

  @Override
  public Collection<Spec> getSpecs() throws IOException {
    Collection<Spec> specs = Lists.newArrayList();
    try {
      getSpecs(this.fsSpecStoreDirPath, specs);
    } catch (Exception e) {
      throw new IOException(e);
    }

    return specs;
  }

  private void getSpecs(Path directory, Collection<Spec> specs) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(directory);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        getSpecs(fileStatus.getPath(), specs);
      } else {
        specs.add(readSpecFromFile(fileStatus.getPath()));
      }
    }
  }

  /***
   * Read and deserialized Spec from a file.
   * @param path File containing serialized Spec.
   * @return Spec
   * @throws IOException
   */
  protected Spec readSpecFromFile(Path path) throws IOException {
    Spec spec;

    try (FSDataInputStream fis = fs.open(path)) {
      spec = this.specSerDe.deserialize(ByteStreams.toByteArray(fis));
    }

    return spec;
  }

  /***
   * Serialize and write Spec to a file.
   * @param specPath Spec file name.
   * @param spec Spec object to write.
   * @throws IOException
   */
  protected void writeSpecToFile(Path specPath, Spec spec) throws IOException {
    byte[] serializedSpec = this.specSerDe.serialize(spec);
    try (FSDataOutputStream os = fs.create(specPath, true)) {
      os.write(serializedSpec);
    }
  }

  /**
   *
   * @param fsSpecStoreDirPath The directory path for specs.
   * @param uri Uri as the identifier of JobSpec
   * @return
   */
  protected Path getPathForURI(Path fsSpecStoreDirPath, URI uri, String version) {
    return PathUtils.addExtension(PathUtils.mergePaths(fsSpecStoreDirPath, new Path(uri)), version);
  }
}
