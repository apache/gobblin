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

import java.util.Map;


/**
 * Utility class for dataset configuration management
 * 
 * @author mitu
 *
 */
public class DatasetUtils {
  public static final String ID_DELEMETER = ".";
  public static final String DATASETS_PREFIX = "config-ds";
  public static final String TAGS_PREFIX = "config-tag";
  public static final String CONSTS_PREFIX = "config-const";
  public static final String ROOT = "root";
  public static final String IMPORTED_TAGS = "imported-tags";

  /**
   * @param urn input urn 
   * @return the parent urn for the input
   */
  public static final String getParentId(String urn) {
    if (urn == null)
      return null;

    int index = urn.lastIndexOf(ID_DELEMETER);
    if (index < 0)
      return ROOT;

    return urn.substring(0, index);
  }

  /**
   * @param urn input urn
   * @return the property key name of the urn. Based on the naming convention, the property name is 
   * the last portion separated by {@link DatasetUtils#ID_DELEMETER}
   */
  public static final String getPropertyName(String urn) {
    if (urn == null)
      return null;

    int index = urn.lastIndexOf(ID_DELEMETER);
    if (index < 0)
      return urn;

    // end with ID_DELEMETER, wrong format
    if (index == urn.length() - 1) {
      throw new IllegalArgumentException("Wrong format: " + urn);
    }
    return urn.substring(index + 1);
  }

  /**
   * @param highProp : high property map
   * @param lowProp  : low property map
   * @return the reference of input highProp which modified to contains all the key/value entries in lowProp map
   * If both key exists in highProp and lowProp, the value in highProp is Not changed
   */
  public static final Map<String, Object> MergeTwoMaps(Map<String, Object> highProp, Map<String, Object> lowProp) {
    if (lowProp == null)
      return highProp;

    if (highProp == null)
      return lowProp;

    for (String key : lowProp.keySet()) {
      if (!highProp.containsKey(key)) {
        highProp.put(key, lowProp.get(key));
      }
    }

    return highProp;
  }

  /**
   * @param urn input urn
   * @return true iff the urn is valid. Starts with either {@link DatasetUtils#TAGS_PREFIX} or {@link DatasetUtils#DATASETS_PREFIX}
   */
  public static final boolean isValidUrn(String urn) {
    return isValidTag(urn) || isValidDataset(urn);
  }

  /**
   * @param urn input urn
   * @return true iff the urn is valid tag. Starts with {@link DatasetUtils#TAGS_PREFIX}
   */
  public static final boolean isValidTag(String urn) {
    if (urn == null)
      return false;

    if (urn.equals(TAGS_PREFIX) || urn.startsWith(TAGS_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }

  /**
   * @param urn input urn
   * @return true iff the urn is valid dataset. Starts with {@link DatasetUtils#DATASETS_PREFIX}
   */
  public static final boolean isValidDataset(String urn) {
    if (urn == null)
      return false;

    if (urn.equals(DATASETS_PREFIX) || urn.startsWith(DATASETS_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }

}
