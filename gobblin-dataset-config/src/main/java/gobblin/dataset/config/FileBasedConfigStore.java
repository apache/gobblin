/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.dataset.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * This class is the implementation for the filed based config store. 
 * @author mitu
 *
 */
public class FileBasedConfigStore implements ConfigStore {

  private static final Logger LOG = Logger.getLogger(FileBasedConfigStore.class);
  public static final String CONF_FILE = "application.conf";
  private final String configLocation;
  private final String scheme;

  private String latestVersion;
  private String loadedConfigVersion;
  private RawConfigMapping rawConfigs;
  private boolean initialized = false;

  /**
   * @param location configuration store location
   * @param scheme the scheme name, example DAI-ETL, DALI, Espresso
   */
  public FileBasedConfigStore(String location, String scheme) {
    this.configLocation = location;
    this.scheme = scheme;

    File[] subfiles = new File(this.configLocation).listFiles();
    List<String> subdirs = new ArrayList<String>();
    for(File f: subfiles){
      if(f.isDirectory()){
        subdirs.add(f.getName());
      }
    }
    
    latestVersion = VersionPattern.getLatestVersion(subdirs);
    if(latestVersion==null){
      throw new RuntimeException("Can not find any valid version in " + this.configLocation);
    }
  }
  
  public FileBasedConfigStore(File file, String scheme) {
    this(file.getAbsolutePath(), scheme);
  }

  @Override
  public String getLatestVersion() {
    return this.latestVersion;
  }

  @Override
  public String getScheme() {
    return this.scheme;
  }

  public synchronized void loadConfigs() {
    loadConfigs(this.getLatestVersion());
  }

  public synchronized void loadConfigs(String version) {
    String configurationFile = this.configLocation + "/" + version + "/" + CONF_FILE;
    LOG.info("Trying to load configuration file: " + configurationFile);
    File configToLoad = new File(configurationFile);
    if(!configToLoad.isFile() || !configToLoad.canRead()){
      throw new IllegalArgumentException("File does not exists : " + configToLoad.getAbsolutePath());
    }
    Config config = ConfigFactory.parseFile(configToLoad).resolve();
    rawConfigs = new RawConfigMapping(config);
    this.loadedConfigVersion = version;
    initialized = true;
  }

  private void initialCheck(String version) {
    if (!this.initialized) {
      this.loadConfigs(version);
    }

    if (!this.loadedConfigVersion.equals(version)) {
      throw new ConfigVersionMissMatchException(String.format("Version loaded is %s, version to query is %s",
          this.loadedConfigVersion, version));
    }
  }
  
  public Config getConfig(String urn) {
    return this.getConfig(urn, this.loadedConfigVersion);
  }

  @Override
  public Config getConfig(String urn, String version) {
    initialCheck(version);
    Map<String, Object> raw = this.rawConfigs.getResolvedProperty(urn);
    return ConfigFactory.parseMap(raw);
  }

  public Map<String, Config> getTaggedConfig(String urn) {
    return this.getTaggedConfig(urn, this.loadedConfigVersion);
  }

  @Override
  public Map<String, Config> getTaggedConfig(String urn, String version) {
    initialCheck(version);
    return this.rawConfigs.getTaggedConfig(urn);
  }

  public List<String> getAssociatedTags(String urn) {
    return this.getAssociatedTags(urn, this.loadedConfigVersion);
  }

  @Override
  public List<String> getAssociatedTags(String urn, String version) {
    initialCheck(version);
    return this.rawConfigs.getAssociatedTags(urn);
  }

}
