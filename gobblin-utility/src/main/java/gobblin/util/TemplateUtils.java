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

import gobblin.configuration.ConfigurationKeys;


public class TemplateUtils {
  /**
   * create a complete property file based on the given template
   */
  public static Properties mergeTemplateWithUserCustomizedFile(Properties template, Properties userCustomized) {
    Properties cleanedTemplate = template;
    if (cleanedTemplate.containsKey(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST)) {
      cleanedTemplate.remove(ConfigurationKeys.REQUIRED_ATRRIBUTES_LIST);
    }

    Properties cleanedUserCustomized = userCustomized ;
    if (cleanedUserCustomized.containsKey(ConfigurationKeys.JOB_TEMPLATE_PATH)) {
      cleanedUserCustomized.remove(ConfigurationKeys.JOB_TEMPLATE_PATH);
    }

    return PropertiesUtils.combineProperties(cleanedTemplate, cleanedUserCustomized);
  }
}
