package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;

public class SingleLinkedListConfigKeyPath implements ConfigKeyPath{
  
  public static final SingleLinkedListConfigKeyPath ROOT = new SingleLinkedListConfigKeyPath(null, "");

  private final ConfigKeyPath parent;
  private final String ownName;
  
  // constructor private, can only create path from ROOT using createChild method
  private SingleLinkedListConfigKeyPath(ConfigKeyPath parent, String name){
    this.parent = parent;
    this.ownName = name;
  }
  
  @Override
  public ConfigKeyPath getParent() {
    return this.parent;
  }

  @Override
  public String getOwnPathName() {
    return this.ownName;
  }

  @Override
  public ConfigKeyPath createChild(String childPathName) {
    return new SingleLinkedListConfigKeyPath(this, childPathName);
  }

  @Override
  public String getAbsolutePathString() {
    if(this.isRootPath()){
      return "";
    }
    
    return this.parent.getAbsolutePathString() + "/" + this.ownName;
  }

  @Override
  public boolean isRootPath() {
    return this == ROOT;
  }
  
  @Override
  public String toString(){
    return this.getAbsolutePathString();
  }

}
