package gobblin.dataset.config;

import java.util.ArrayList;
import java.util.List;

/*
 
 config-stores{
   ETL {
     class=gobblin.dataset.config.HDFSConfigStore
     path=/...
   }
   
   DALI {
     class=gobblin.dataset.config.HttpConfigStore
     url=http://...
   }
 }
 
 */
public class ConfigClient {

  private void initialization(){
    
  }
  
  public List<String> getAllSchemes(){
    return new ArrayList<String>();
  }
  
  public ConfigAccessor getConfigAccessor(String scheme){
    /*
     * 1. initialize the ConfigStore 
     * 2. retrieve the latest config version by calling ConfigStore.getLatestVersion();
     */
    return null;
  }
}
