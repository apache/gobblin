/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.util.Properties;


/**
 * A utility class for {@link Properties} objects.
 */
public class PropertiesUtils {

  /**
   * Combine a variable number of {@link Properties} into a single {@link Properties}.
   */
  public static Properties combineProperties(Properties... properties) {
    Properties combinedProperties = new Properties();
    for (Properties props : properties) {
      combinedProperties.putAll(props);
    }
    return combinedProperties;
  }
}
