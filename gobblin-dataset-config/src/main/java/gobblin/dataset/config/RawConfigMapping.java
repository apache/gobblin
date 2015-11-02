/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.dataset.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;


/**
 * This class is used to transfer com.typesafe.config.Config to a raw config mapping and use that mapping to resolve the 
 * property for dataset/tags.
 * @author mitu
 *
 */

public class RawConfigMapping {

  private static final Logger LOG = Logger.getLogger(RawConfigMapping.class);
  private final Map<String, Map<String, Object>> configMap = new HashMap<String, Map<String, Object>>();
  private final Config allConfigs;

  public RawConfigMapping(Config allConfigs) {
    this.allConfigs = allConfigs;
    initialMapping();
  }

  /**
   * This function is used to transfer resolved com.typesafe.config.Config into config mapping
   * 
   * The key of configMap will be the dataset/tag names
   * The value of the configMap will be the configuration for the corresponding key
   */
  private void initialMapping() {
    for (Map.Entry<String, ConfigValue> entry : allConfigs.entrySet()) {
      // constant variables should be resolved already
      if (entry.getKey().startsWith(DatasetUtils.CONSTS_PREFIX + DatasetUtils.ID_DELEMETER)) {
        continue;
      }

      if (DatasetUtils.isValidUrn(entry.getKey())) {

        String parentId = DatasetUtils.getParentId(entry.getKey());
        String propertyName = DatasetUtils.getPropertyName(entry.getKey());
        Object value = entry.getValue().unwrapped();

        // check import-tags are valid entry
        checkAssociatedTags(propertyName, value);

        Map<String, Object> submap = configMap.get(parentId);
        if (submap == null) {
          submap = new HashMap<String, Object>();
          configMap.put(parentId, submap);
        }
        submap.put(propertyName, value);
      }
      // invalid urn
      else {
        throw new IllegalArgumentException("Invalid urn " + entry.getKey());
      }
    }
  }

  /**
   * @param key input urn
   * @param value is the configuration collection for input urn
   * @throws RuntimeException if the value of {@DatasetUtils.IMPORTED_TAGS} is invalid tag
   */
  private void checkAssociatedTags(String key, Object value) {
    if (!key.equals(DatasetUtils.IMPORTED_TAGS))
      return;

    if (value instanceof String) {
      String tag = (String) value;
      if (!DatasetUtils.isValidTag(tag)) {
        throw new RuntimeException("Invalid tag: " + tag);
      }
    }

    if (value instanceof List) {
      @SuppressWarnings("unchecked")
      List<String> tags = (List<String>) value;
      for (String tag : tags) {
        if (!DatasetUtils.isValidTag(tag)) {
          throw new RuntimeException("Invalid tag: " + tag);
        }
      }
    }
  }

  /**
   * @param urn input run
   * @return the closest urn which is specified in the configMap
   * 
   * say we have following urns as key in the configMap:
   * conf-ds.high-priority
   * conf-ds.high-priority.nertz.tarcking.big-event
   * 
   * getAdjustedUrn(conf-ds.high-priority.nertz.tracking) will return conf-ds.high-priority 
   */
  public String getAdjustedUrn(String urn) {
    if (urn.equals(DatasetUtils.ROOT)) {
      return urn;
    }

    if (!DatasetUtils.isValidUrn(urn)) {
      throw new IllegalArgumentException("Invalid urn:" + urn);
    }

    if (configMap.containsKey(urn)) {
      return urn;
    }

    return getAdjustedUrn(DatasetUtils.getParentId(urn));
  }

  /**
   * This function will be used to detect the circular dependency between ancestor to descendant
   * @param urn the input urn
   * @param adjusted the adjusted urn by function getAdjustedUrn(urn)
   * @return the list of urns from input urn (included ) to input adjusted ( excluded )
   * 
   * For example: urn is conf-ds.a1.a2.a3.a4
   * adjusted urn is     conf-da.a1.a2
   * return result will be {conf-ds.a1.a2.a3.a4, conf-ds.a1.a2.a3}
   */
  private List<String> getUrnTillAdjustedUrn(String urn, String adjusted) {
    List<String> res = new ArrayList<String>();
    if (urn.equals(adjusted)) {
      return res;
    }

    res.add(urn);

    String parentId = DatasetUtils.getParentId(urn);
    while (!parentId.equals(adjusted)) {
      res.add(parentId);
      parentId = DatasetUtils.getParentId(parentId);
    }
    return res;
  }

  /**
   * @param urn input urn
   * @return the rawProperty of the input urn which do not include the property from ancestors or imported-tags
   */
  public Map<String, Object> getRawProperty(String urn) {
    String adjustedUrn = this.getAdjustedUrn(urn);
    Map<String, Object> res = configMap.get(adjustedUrn);

    if (res == null) {
      return Collections.emptyMap();
    }
    // need to return deep copy as the map object could be changed later
    return new HashMap<String, Object>(res);
  }

  @SuppressWarnings("unchecked")
  /**
   * @param urn input urn
   * @return the directly imported tags for this urn which do not include the tags inherent from ancestors or associated tags.
   * The order in the result list determines the conflict resolution priority. The lower indexed one have higher priority.
   */
  public List<String> getRawTags(String urn) {
    Map<String, Object> self = getRawProperty(urn);
    Object tags = self.get(DatasetUtils.IMPORTED_TAGS);

    List<String> res = new ArrayList<String>();
    if (tags == null)
      return res;

    if (tags instanceof String) {
      res.add((String) tags);
    } else {
      // need to return deep copy
      // and added it in reverse order as by definition, the latter imported-tag has high priority
      List<String> tmp = (List<String>) tags;
      for (int i = tmp.size() - 1; i >= 0; i--) {
        res.add(tmp.get(i));
      }
    }

    // tag refer to self
    for (String s : res) {
      if (s.equals(urn)) {
        throw new TagCircularDependencyException("Tag associated with self or children " + s);
      }
    }

    return res;
  }

  /**
   * @param urn input urn
   * @return All the tags directly imported by this urn or inherent from ancestors or associated tags.
   */
  public List<String> getAssociatedTags(String urn) {
    if (urn.equals(DatasetUtils.ROOT)) {
      return Collections.emptyList();
    }
    return this.getAssociatedTagsWithCheck(urn, new HashSet<String>());
  }

  /**
   * @param urn
   * @param previousTags the tags already been imported
   * @return All the tags directly imported by this urn or inherent from ancestors or associated tags.
   * @throws TagCircularDependencyException if there is a circular dependence in the chain
   */
  private List<String> getAssociatedTagsWithCheck(String urn, Set<String> previousTags) {
    List<String> self = getRawTags(urn);
    List<String> res = new ArrayList<String>();

    // add tags associated with each directly imported tag
    for (String s : self) {
      if (previousTags.contains(s)) {
        throw new TagCircularDependencyException("Circular dependence for tag " + s);
      }

      res.add(s);
      Set<String> combined = new HashSet<String>(previousTags);
      combined.add(s);

      res.addAll(getAssociatedTagsWithCheck(s, combined));
    }

    // add tags inherent from parentId
    String parentId = DatasetUtils.getParentId(getAdjustedUrn(urn));
    if (!parentId.equals(DatasetUtils.ROOT)) {
      // check circular dependency for self chain
      Set<String> selfChain = new HashSet<String>(getUrnTillAdjustedUrn(urn, parentId));
      List<String> ancestorTags = getAssociatedTagsWithCheck(parentId, selfChain);
      res.addAll(ancestorTags);
    }

    return dedup(res);
  }

  /**
   * @param input a list of tags
   * @return the deep copy of the input where the duplicated value got removed.
   * 
   * Example, one dataset imported tag1 and tag2, both tag1 and tag2 imported tagx. This is a valid senario but need
   * to remove the duplicated tagx
   */
  private List<String> dedup(List<String> input) {
    Set<String> processed = new HashSet<String>();
    List<String> res = new ArrayList<String>();
    for (String s : input) {
      if (processed.contains(s)) {
        continue;
      }

      res.add(s);
      processed.add(s);
    }
    return res;
  }

  // get resolved property: self union ancestor union tags
  /**
   * @param urn input urn
   * @return the rawProperty of the input urn which does include the property from ancestors or imported-tags
   */
  public Map<String, Object> getResolvedProperty(String urn) {
    // self need to be deep copy as it will be changed
    Map<String, Object> self = getRawProperty(urn);

    // get all property from tags
    List<String> tags = getAssociatedTags(urn);
    for (String t : tags) {
      DatasetUtils.mergeTwoMaps(self, getResolvedProperty(t));
    }

    // get all property from parent
    String parentId = DatasetUtils.getParentId(getAdjustedUrn(urn));
    Map<String, Object> ancestor = null;
    if (parentId.equals(DatasetUtils.ROOT)) {
      ancestor = this.getRawProperty(parentId);
    } else {
      ancestor = this.getResolvedProperty(parentId);
    }

    Map<String, Object> res = DatasetUtils.mergeTwoMaps(self, ancestor);

    // as imported-tags will be overwrite or the order is not correct once the map got merged
    // do not return the imported-tags, instead, user should use getAssociatedTags(String urn) 
    // to get the tags for this urn
    if (res != null) {
      res.remove(DatasetUtils.IMPORTED_TAGS);
    }
    return res;
  }

  /**
   * @param inputTag, input tag
   * @return the Map whose key is the dataset which has input tag associated, not include descendant datasets,
   *  value is resolved config for the corresponding key. If input is dataset, result is empty as dataset is not taggable
   */
  public Map<String, Config> getTaggedConfig(String inputTag) {
    if (!DatasetUtils.isValidTag(inputTag))
      return null;

    Map<String, Config> res = new HashMap<String, Config>();

    for (String urnKey : this.configMap.keySet()) {
      List<String> rawTags = this.getRawTags(urnKey);
      if (rawTags != null) {
        for (String tag : rawTags) {
          if (tag.equals(inputTag)) {
            res.put(urnKey, ConfigFactory.parseMap(this.getResolvedProperty(urnKey)));
          }
        }
      }
    }
    return res;
  }
}
