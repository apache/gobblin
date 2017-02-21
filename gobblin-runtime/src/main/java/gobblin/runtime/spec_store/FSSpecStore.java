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

package gobblin.runtime.spec_store;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.api.SpecStore;
import gobblin.util.PathUtils;


/**
 * The Spec Store for file system to persist the Spec information.
 * Note:
 * 1. This implementation has no support for caching.
 * 2. This implementation does not performs implicit version management.
 *    For implicit version management, please use a wrapper FSSpecStore.
 */
@Slf4j
public class FSSpecStore implements SpecStore {

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  protected final Config sysConfig;
  protected final FileSystem fs;
  protected final String fsSpecStoreDir;
  protected final Path fsSpecStoreDirPath;

  public FSSpecStore(GobblinInstanceEnvironment env)
      throws IOException {
    this(env.getSysConfig().getConfig());
  }

  public FSSpecStore(Config sysConfig)
      throws IOException {
    Preconditions.checkArgument(sysConfig.hasPath(ConfigurationKeys.SPECSTORE_FS_DIR_KEY),
        "FS SpecStore path must be specified.");

    this.sysConfig = sysConfig;
    this.fsSpecStoreDir = this.sysConfig.getString(ConfigurationKeys.SPECSTORE_FS_DIR_KEY);
    this.fsSpecStoreDirPath = new Path(this.fsSpecStoreDir);
    try {
      this.fs = this.fsSpecStoreDirPath.getFileSystem(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException("Unable to detect job config directory file system: " + e, e);
    }
  }

  @Override
  public boolean exists(URI specUri) throws IOException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");

    FileStatus[] fileStatuses = fs.listStatus(this.fsSpecStoreDirPath);
    for (FileStatus fileStatus : fileStatuses) {
      if (StringUtils.startsWith(fileStatus.getPath().getName(), specUri.toString())) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void addSpec(Spec spec) throws IOException {
    Preconditions.checkArgument(null != spec, "Spec should not be null");

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

    try {
      return deleteSpec(specUri, getSpec(specUri).getVersion());
    } catch (SpecNotFoundException e) {
      throw new IOException(String.format("Issue in removing Spec: %s", specUri), e);
    }
  }

  @Override
  public boolean deleteSpec(URI specUri, String version) throws IOException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");
    Preconditions.checkArgument(null != version, "Version should not be null");

    try {
      Path specPath = getPathForURI(this.fsSpecStoreDirPath, specUri, version);

      if (fs.exists(specPath)) {
        return fs.delete(specPath, false);
      } else {
        log.warn("No file with URI:" + specUri + " is found. Deletion failed.");
        return false;
      }
    } catch (IOException e) {
      throw new IOException(String.format("Issue in removing Spec: %s for Version: %s", specUri, version), e);
    }
  }

  @Override
  public Spec updateSpec(Spec spec) throws IOException, SpecNotFoundException {
    Preconditions.checkArgument(null != spec, "Spec should not be null");

    Path specPath = getPathForURI(this.fsSpecStoreDirPath, spec.getUri(), spec.getVersion());
    writeSpecToFile(specPath, spec);

    return spec;
  }

  @Override
  public Spec getSpec(URI specUri) throws IOException, SpecNotFoundException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");

    Collection<Spec> specs = getAllVersionsOfSpec(specUri);
    Spec highestVersionSpec = null;

    for (Spec spec : specs) {
      if (null == highestVersionSpec) {
        highestVersionSpec = spec;
      } else if (spec.getVersion().compareTo(spec.getVersion()) > 0) {
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
  public Collection<Spec> getAllVersionsOfSpec(URI specUri) throws IOException, SpecNotFoundException {
    Preconditions.checkArgument(null != specUri, "Spec URI should not be null");

    Collection<Spec> specs = Lists.newArrayList();
    FileStatus[] fileStatuses = fs.listStatus(this.fsSpecStoreDirPath);
    for (FileStatus fileStatus : fileStatuses) {
      if (StringUtils.startsWith(fileStatus.getPath().getName(), specUri.toString())) {
        specs.add(readSpecFromFile(fileStatus.getPath()));
      }
    }

    if (specs.size() == 0) {
      throw new SpecNotFoundException(specUri);
    }

    return specs;
  }

  @Override
  public Collection<Spec> getSpecs() throws IOException {
    Collection<Spec> specs = Lists.newArrayList();
    FileStatus[] fileStatuses = fs.listStatus(this.fsSpecStoreDirPath);
    for (FileStatus fileStatus : fileStatuses) {
      specs.add(readSpecFromFile(fileStatus.getPath()));
    }

    return specs;
  }

  /***
   * Read and deserialized Spec from a file.
   * @param path File containing serialized Spec.
   * @return Spec
   * @throws IOException
   */
  protected Spec readSpecFromFile(Path path) throws IOException {
    Spec spec = null;
    FSDataInputStream fis = fs.open(path);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    try {
      spec = gson.fromJson(br, Spec.class);
    } finally {
      fis.close();
      br.close();
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
    if (fs.exists(specPath)) {
      fs.delete(specPath, true);
    }
    FSDataOutputStream os = fs.create(specPath);
    byte[] serializedSpec = gson.toJson(spec).getBytes(Charset.forName("UTF-8"));
    try {
      os.write(serializedSpec);
    } finally {
      if (null != os) {
        os.close();
      }
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
