package gobblin.dataset.config;

import java.util.*;

import com.typesafe.config.*;
public class TagNodeImpl extends ConfigNodeImpl{

  private Config tagsConfig;
  private String tagId;
  
  public TagNodeImpl(Config tagsConfig, String tagId){
    this.tagsConfig = tagsConfig;
    this.tagId = tagId;
  }
 
  public ConfigNode getParent(){
    String parentId = ConfigNodeUtils.getParentId(this.tagId);
    if(parentId == null) return null;
    
    return new TagNodeImpl(this.tagsConfig, parentId);
  }
  
  public Config getConfig(){
//    Map<String, Object> result = new HashMap<String, Object>();
//    Set<Map.Entry<String, ConfigValue>> entrySet = this.tagsConfig.entrySet();
//    for(Map.Entry<String, ConfigValue> entry: entrySet){
//      if(ConfigNodeUtils.belongsToConfigNode(this.tagId, entry.getKey())){
//        result.put(entry.getKey().substring(this.tagId.length()+1), entry.getValue().unwrapped());
//        System.out.println("BBB 1 " + result.entrySet().size());
//      }
//    }
    
    Map<String, Object> self = Repository.getInstance().getRawProperty(tagId);
    Map<String, Object> parent = Repository.getInstance().getRawProperty(ConfigNodeUtils.getParentId(tagId));
    ConfigNodeUtils.MergeTwoMaps(self, parent);
    return ConfigFactory.parseMap(self);
  }
  
}
