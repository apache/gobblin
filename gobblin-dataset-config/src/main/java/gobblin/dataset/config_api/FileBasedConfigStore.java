package gobblin.dataset.config_api;

import java.util.List;
import java.util.Map;

import java.io.*;

import com.typesafe.config.*;


public class FileBasedConfigStore implements ConfigStore {

  public static final String CONF_FILE = "application.conf";
  private final String configLocation;
  private final String scheme;

  private String latestVersion;
  private String loadedConfigVersion;
  private RawConfigMapping rawConfigs;
  private boolean initialized = false;

  public FileBasedConfigStore(String location, String scheme) {
    this.configLocation = location;
    this.scheme = scheme;

    // TODO
    this.latestVersion = "v1.0";
  }

  @Override
  public String getLatestVersion() {
    return this.latestVersion;
  }

  @Override
  public String getScheme() {
    return this.scheme;
  }

  @Override
  public synchronized void loadConfigs() {
    loadConfigs(this.getLatestVersion());
  }

  @Override
  public synchronized void loadConfigs(String version) {
    Config config = ConfigFactory.parseFile(new File(this.configLocation + "/" + version + "/" + CONF_FILE));
    rawConfigs = new RawConfigMapping(config);
    this.loadedConfigVersion = version;
    initialized = true;
  }

  @Override
  public Config getConfig(String urn) {
    return this.getConfig(urn, this.loadedConfigVersion);
  }

  @Override
  public Config getConfig(String urn, String version) {
    if (!this.initialized) {
      this.loadConfigs(version);
    }

    if (!this.loadedConfigVersion.equals(version)) {
      throw new ConfigVersionMissMatchException(String.format("Version loaded is %s, version to query is %s",
          this.loadedConfigVersion, version));
    }
    
    if (!DatasetUtils.isValidUrn(urn)){
      throw new IllegalArgumentException("Invalid urn: " + urn);
    }
    
    Map<String, Object> raw = this.rawConfigs.getResolvedProperty(urn);
    return ConfigFactory.parseMap(raw);
  }

  @Override
  public Map<String, Config> getTaggedConfig(String urn) {
    return this.getTaggedConfig(urn, this.loadedConfigVersion);
  }

  @Override
  public Map<String, Config> getTaggedConfig(String urn, String version) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getAssociatedTags(String urn) {
    return this.getAssociatedTags(urn, this.loadedConfigVersion);
  }

  @Override
  public List<String> getAssociatedTags(String urn, String version) {
    return this.rawConfigs.getRawTags(urn);
  }

}
