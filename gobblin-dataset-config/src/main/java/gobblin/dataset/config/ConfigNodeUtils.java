package gobblin.dataset.config;

import java.util.*;

public class ConfigNodeUtils {

  public static final String ID_DELEMETER = ".";
  
  public static final String getParentId(String urn){
    if (urn == null) return null;
    
    int index = urn.lastIndexOf(ID_DELEMETER);
    if(index<0) return null;
    
    return urn.substring(0, index);
  }
  
  public static final String getPropertyName(String fullPropertyName){
    if (fullPropertyName == null) return null;
    
    int index = fullPropertyName.lastIndexOf(ID_DELEMETER);
    if(index<0) return fullPropertyName;
    
    // end with ID_DELEMETER, wrong format
    if(index == fullPropertyName.length()-1){
      throw new IllegalArgumentException("Wrong format: " + fullPropertyName);
    }
    return fullPropertyName.substring(index+1);
  }
  
  public static final void MergeTwoMaps(Map<String, Object> highProp, Map<String, Object> lowProp){
    if(lowProp == null) return;
    
    for(String key: lowProp.keySet()){
      if(!highProp.containsKey(key)){
        highProp.put(key, lowProp.get(key));
      }
    }
  }
  
  public static boolean belongsToConfigNode(String nodeId, String key){
    if(nodeId==null || key==null) return false;
    
    if(!key.startsWith(nodeId + ID_DELEMETER)){
      return false;
    }
    
    int lastIndex = key.lastIndexOf(ID_DELEMETER);
    return lastIndex == nodeId.length() ;
  }
}
