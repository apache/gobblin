package gobblin.config.regressiontest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import gobblin.config.store.api.ConfigKeyPath;

public class ValidationUtils {

  public static void validateConfigs(Config config1, Config config2){
    displayConfig(config1, "Config1 ");
    displayConfig(config2, "Config2 ");
    Preconditions.checkState(config1.entrySet().size() == config2.entrySet().size());
    
    for(Map.Entry<String, ConfigValue> entry: config1.entrySet()){
      Preconditions.checkState(entry.getValue().equals(config2.getValue(entry.getKey())));
    }
  }
  
  public static void validateConfigKeyPathsWithOrder(List<ConfigKeyPath> list1, List<ConfigKeyPath> list2){
    display(list1, "LIST1 ");
    display(list2, "LIST2 ");
    if(!continueCheck(list1,list2)){
      return;
    }

    
    for(int i=0; i<list1.size(); i++){
      Preconditions.checkState(list1.get(i).equals(list2.get(i)));
    }
  }
  
  public static void validateConfigKeyPathsWithOutOrder(Collection<ConfigKeyPath> collection1, 
      Collection<ConfigKeyPath> collection2){
    display(collection1, "COL1 ");
    display(collection2, "COL2 ");
    if(!continueCheck(collection1,collection2)){
      return;
    }
    
    
    Set<ConfigKeyPath> set1 = new HashSet<>(collection1);
    Set<ConfigKeyPath> set2 = new HashSet<>(collection2);
    
    Preconditions.checkState(set1.size() == set2.size());
    for(ConfigKeyPath path1: set1){
      Preconditions.checkState(set2.contains(path1));
    }
  }
  
  // treat empty collection the same as null
  private static boolean continueCheck (Collection<ConfigKeyPath> collection1, 
      Collection<ConfigKeyPath> collection2 ){
    if(collection1 == null) {
      if(collection2 == null || collection2.size()==0){
        return false;
      }
      
      throw new RuntimeException("List size not match");
    }
    
    if(collection2 == null) {
      if(collection1 == null || collection1.size()==0){
        return false;
      }
      
      throw new RuntimeException("List size not match");
    }
    
    Preconditions.checkState(collection1.size() == collection2.size());
    return true;
  }
  
  private static void display(Collection<ConfigKeyPath> col, String prefix){
    if(col == null) return;
    for(ConfigKeyPath path: col){
      System.out.println(prefix + " " + path);
    }
  }
  
  private static void displayConfig(Config c, String prefix){
    for(Map.Entry<String, ConfigValue> entry: c.entrySet()){
      System.out.println(prefix + " key:" + entry.getKey() + "-> " + entry.getValue());
    }
  }
}
