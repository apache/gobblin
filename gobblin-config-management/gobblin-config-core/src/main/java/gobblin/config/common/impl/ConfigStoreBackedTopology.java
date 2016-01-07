package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreWithImportedBy;
import gobblin.config.store.api.ConfigStoreWithImportedByRecursively;
import gobblin.config.store.api.ConfigStoreWithResolution;

import java.util.Collection;
import java.util.List;

public class ConfigStoreBackedTopology implements ConfigStoreTopologyIntf{
  
  private final ConfigStore cs;
  private final String version;
  
  public ConfigStoreBackedTopology(ConfigStore cs, String version){
    this.cs = cs;
    this.version = version;
  }
  
  public ConfigStore getConfigStore(){
    return this.cs;
  }
  
  public String getVersion(){
    return this.version;
  }

  @Override
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey) {
    return this.cs.getChildren(configKey, this.version);
  }

  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey) {
    return this.cs.getOwnImports(configKey, this.version);
  }

  @Override
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey) {
    if(this.cs instanceof ConfigStoreWithImportedBy){
      return ((ConfigStoreWithImportedBy)this.cs).getImportedBy(configKey, this.version);
    }
    
    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }

  @Override
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey) {
    if(this.cs instanceof ConfigStoreWithResolution){
      return ((ConfigStoreWithResolution)this.cs).getImportsRecursively(configKey, this.version);
    }
    
    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }

  @Override
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey) {
    if(this.cs instanceof ConfigStoreWithImportedByRecursively){
      return ((ConfigStoreWithImportedByRecursively)this.cs).getImportedByRecursively(configKey, this.version);
    }
    
    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }
}
