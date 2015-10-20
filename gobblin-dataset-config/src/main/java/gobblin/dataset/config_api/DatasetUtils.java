package gobblin.dataset.config_api;

import java.util.*;


public class DatasetUtils {
  public static final String ID_DELEMETER = ".";
  public static final String DATASETS_PREFIX = "config-ds";
  public static final String TAGS_PREFIX = "config-tag";
  public static final String ROOT = "root";
  public static final String IMPORTED_TAGS = "imported-tags";

  public static final String getParentId(String urn) {
    if (urn == null)
      return null;

    int index = urn.lastIndexOf(ID_DELEMETER);
    if (index < 0)
      return ROOT;

    return urn.substring(0, index);
  }

  public static final String getPropertyName(String fullPropertyName) {
    if (fullPropertyName == null)
      return null;

    int index = fullPropertyName.lastIndexOf(ID_DELEMETER);
    if (index < 0)
      return fullPropertyName;

    // end with ID_DELEMETER, wrong format
    if (index == fullPropertyName.length() - 1) {
      throw new IllegalArgumentException("Wrong format: " + fullPropertyName);
    }
    return fullPropertyName.substring(index + 1);
  }

  public static final Map<String,Object> MergeTwoMaps(Map<String, Object> highProp, Map<String, Object> lowProp) {
    if (lowProp == null)
      return highProp;
    
    if(highProp == null)
      return lowProp;

    for (String key : lowProp.keySet()) {
      if (!highProp.containsKey(key)) {
        highProp.put(key, lowProp.get(key));
      }
    }
    
    return highProp;
  }

  public static final boolean isValidUrn(String urn) {
    return isValidTag(urn) || isValidDataset(urn);
  }
  
  public static final boolean isValidTag(String urn){
    if (urn == null)
      return false;

    if (urn.equals(TAGS_PREFIX) || urn.startsWith(TAGS_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }
  
  public static final boolean isValidDataset(String urn){
    if (urn == null)
      return false;

    if (urn.equals(DATASETS_PREFIX) || urn.startsWith(DATASETS_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }
}
