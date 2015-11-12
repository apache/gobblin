package gobblin.config.configstore.impl;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreFactory;

public class ConfigStoreFactoryUsingProperty implements ConfigStoreFactory {
  
  private final String propertyLocation;
  private final Map<String, String> stores = new HashMap<String, String>();
  
  public static final String STORES = "stores";
  public static final String SCHEME_NAME = "store_scheme_name";
  public static final String LOCATION = "store_location";
  
  public ConfigStoreFactoryUsingProperty (String propertyLocation ){
    this.propertyLocation = propertyLocation;
    Config c = ConfigFactory.parseFile(new File(this.propertyLocation));
    List<? extends Object> l = c.getAnyRefList(STORES);

    for(Object o: l){
      if(o instanceof Map){
        Map<String, String> m = (Map<String, String>)o;
        this.stores.put(m.get(SCHEME_NAME), m.get(LOCATION));
      }
      else {
        throw new RuntimeException("Invalid type " + o.getClass());
      }
    }
  }

  @Override
  public Collection<String> getConfigStoreSchemes() {
    return this.stores.keySet();
  }

  @Override
  public ConfigStore getConfigStore(String scheme) {
    if(!this.stores.containsKey(scheme)) return null;
    
    // TBD: need to based on configuration to construct 
    return new ETLHdfsConfigStore(scheme, this.stores.get(scheme));
  }
}
