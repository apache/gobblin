package gobblin.dataset.config;

import java.util.*;

import java.io.*;
import com.typesafe.config.*;


public class Repository {

  private Repository() {

  }

  private final Map<String, Map<String, Object>> configMap = new HashMap<String, Map<String, Object>>();
  private static boolean initialized = false;
  public static final Repository _instance = new Repository();
  public static final String ROOT = "root";

  public static synchronized Repository getInstance() {
    if (!initialized) {
      throw new IllegalArgumentException("Repository not initialized, need to call getInstance(File configs)");
    }
    return _instance;
  }

  public static synchronized Repository getInstance(File configFile) {

    _instance.initialRepository(configFile);
    return _instance;
  }

  private void initialRepository(File configFile) {
    Config config = ConfigFactory.parseFile(configFile);

    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      String parentId = ConfigNodeUtils.getParentId(entry.getKey());
      String propertyName = ConfigNodeUtils.getPropertyName(entry.getKey());
      Object value = entry.getValue().unwrapped();

      if (parentId == null) {
        parentId = ROOT;
      }

      Map<String, Object> submap = configMap.get(parentId);
      if (submap == null) {
        submap = new HashMap<String, Object>();
        configMap.put(parentId, submap);
      }
      submap.put(propertyName, value);
    }
    
    initialized = true;
  }

  public Map<String, Object> getRawProperty(String id) {
    return configMap.get(id);
  }
}
