package gobblin.dataset.config;

import java.util.ArrayList;
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

  @Override
  public synchronized void loadConfigs() {
    loadConfigs(this.getLatestVersion());
  }

  @Override
  public synchronized void loadConfigs(String version) {
    //System.out.println("Abs path is " + (this.configLocation + "/" + version + "/" + CONF_FILE));
    File configToLoad = new File(this.configLocation + "/" + version + "/" + CONF_FILE);
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
  @Override
  public Config getConfig(String urn) {
    return this.getConfig(urn, this.loadedConfigVersion);
  }

  @Override
  public Config getConfig(String urn, String version) {
    initialCheck(version);
    Map<String, Object> raw = this.rawConfigs.getResolvedProperty(urn);
    return ConfigFactory.parseMap(raw);
  }

  @Override
  public Map<String, Config> getTaggedConfig(String urn) {
    return this.getTaggedConfig(urn, this.loadedConfigVersion);
  }

  @Override
  public Map<String, Config> getTaggedConfig(String urn, String version) {
    return this.rawConfigs.getTaggedConfig(urn);
  }

  @Override
  public List<String> getAssociatedTags(String urn) {
    return this.getAssociatedTags(urn, this.loadedConfigVersion);
  }

  @Override
  public List<String> getAssociatedTags(String urn, String version) {
    initialCheck(version);
    return this.rawConfigs.getAssociatedTags(urn);
  }

}
