package gobblin.config.regressiontest;

import java.util.Collection;
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
    Preconditions.checkState(config1.entrySet().size() == config2.entrySet().size());
    
    for(Map.Entry<String, ConfigValue> entry: config1.entrySet()){
      Preconditions.checkState(entry.getValue().equals(config2.getValue(entry.getKey())));
    }
  }
  
  public static void validateConfigKeyPathsWithOrder(List<ConfigKeyPath> list1, List<ConfigKeyPath> list2){
    Preconditions.checkState(list1.size() == list2.size());
    
    for(int i=0; i<list1.size(); i++){
      Preconditions.checkState(list1.get(i).equals(list2.get(i)));
    }
  }
  
  public static void validateConfigKeyPathsWithOutOrder(Collection<ConfigKeyPath> collection1, 
      Collection<ConfigKeyPath> collection2){
    Preconditions.checkState(collection1.size() == collection2.size());
    
    Set<ConfigKeyPath> set1 = new HashSet<>(collection1);
    Set<ConfigKeyPath> set2 = new HashSet<>(collection2);
    
    Preconditions.checkState(set1.size() == set2.size());
    for(ConfigKeyPath path1: set1){
      Preconditions.checkState(set2.contains(path1));
    }
  }
}
