package gobblin.dataset.config_api;

import java.util.*;

import com.typesafe.config.*;

import org.apache.log4j.Logger;


public class RawConfigMapping {

  private static final Logger LOG = Logger.getLogger(RawConfigMapping.class);
  private final Map<String, Map<String, Object>> configMap = new HashMap<String, Map<String, Object>>();
  private final Config allConfigs;

  public RawConfigMapping(Config allConfigs) {
    this.allConfigs = allConfigs;
    initialMapping();
  }

  private void initialMapping() {
    for (Map.Entry<String, ConfigValue> entry : allConfigs.entrySet()) {
      if (DatasetUtils.isValidUrn(entry.getKey())) {

        String parentId = DatasetUtils.getParentId(entry.getKey());
        String propertyName = DatasetUtils.getPropertyName(entry.getKey());
        Object value = entry.getValue().unwrapped();

        Map<String, Object> submap = configMap.get(parentId);
        if (submap == null) {
          submap = new HashMap<String, Object>();
          configMap.put(parentId, submap);
        }
        submap.put(propertyName, value);
      }
      // invalid urn
      else {
        LOG.error("Invalid urn " + entry.getKey());
      }
    }

  }

  // return the closest urn which is specified in the config
  public String getAdjustedUrn(String urn){
    if(urn.equals(DatasetUtils.ROOT)){
      return urn;
    }

    if(!DatasetUtils.isValidUrn(urn)){
      throw new IllegalArgumentException("Invalid urn:" + urn);
    }

    if(configMap.containsKey(urn)){
      return urn;
    }

    return getAdjustedUrn(DatasetUtils.getParentId(urn));
  }

  // get own properties, not resolved yet
  public Map<String, Object> getRawProperty(String urn) {
    String adjustedUrn = this.getAdjustedUrn(urn);
    Map<String, Object> res = configMap.get(adjustedUrn);
    
    if(res == null){
      return new HashMap<String, Object>();
    }
    return new HashMap<String, Object>(res);
  }

  @SuppressWarnings("unchecked")
  public List<String> getRawTags(String urn){
    Map<String, Object> self = getRawProperty(urn);
    Object tags = self.get(DatasetUtils.IMPORTED_TAGS);

    if(tags!=null)
      System.out.println(tags.getClass());

    List<String> res=new ArrayList<String>();
    if(tags instanceof String){
      res.add((String)tags);
    }
    else {
      res.addAll((List<String>)tags);
    }


    for(String tag: res){
      if(!DatasetUtils.isValidUrn(tag)){
        throw new RuntimeException("Invalid urn: " + tag);
      }

      if(!tag.startsWith(DatasetUtils.TAGS_PREFIX)){
        throw new RuntimeException("Invalid tag: " + tag);
      }
    }
    return res;
  }

  // get resolved property: self union ancestor union tags
  public Map<String, Object> getResolvedProperty(String urn){
    Map<String, Object> self = getRawProperty(urn);

    String parentId = DatasetUtils.getParentId(getAdjustedUrn(urn));
    Map<String, Object> ancestor = null;
    if(parentId.equals(DatasetUtils.ROOT)){
      ancestor = this.getRawProperty(parentId);
    }
    else {
      ancestor = this.getResolvedProperty(parentId);
    }

    Map<String, Object> res = DatasetUtils.MergeTwoMaps(self, ancestor);
    if(res!=null){
      res.remove(DatasetUtils.IMPORTED_TAGS);
    }
    return res;
  }
}
