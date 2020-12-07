/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.typedconfig;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;


public class TypedConfig {

  public TypedConfig(Properties prop) {
    if (prop != null) {
      fulfillFields(this.getClass(), prop);
    }
  }

  @SneakyThrows
  private void fulfillFields(Class<? extends TypedConfig> clazz, Properties prop) {
    if (!clazz.equals(TypedConfig.class)) {
      this.fulfillFields((Class<? extends TypedConfig>) clazz.getSuperclass(), prop);
    }
    for (Field field : clazz.getDeclaredFields()) {
      if (field.getAnnotations().length == 0) {
        continue;
      }
      Object defaultValue = pickupDefaultValue(field); // get default value which is in config class
      Object configValue = pickupValueByKey(field, prop); // get ini file config value by key
      if (configValue == null) {
        configValue = pickupValueByAlias(field, prop); // by alias (2nd key)
      }
      if (configValue == null) {
        configValue = defaultValue;
      }
      if (configValue != null) {
        configValue = ConstraintUtil.constraint(field, configValue);
        field.set(this, convert(configValue, field.getType()));
      }
    }
  }

  private Object pickupDefaultValue(Field field) {
    Default defaultAnn = field.getAnnotation(Default.class);
    if (defaultAnn == null) {
      return null;
    }
    return defaultAnn.value(); // the value was put in source code instead of ini file
  }

  private Object pickupValueByAlias(Field field, Properties prop) {
    Alias alias = field.getAnnotation(Alias.class);
    if (alias == null) {
      return null;
    }
    return prop.get(alias.value()); // get ini config value by alias(2nd key)
  }

  private Object pickupValueByKey(Field field,  Properties prop) {
    Key key = field.getAnnotation(Key.class);
    if (key == null) {
      return null;
    }
    return prop.get(key.value()); // get ini config value by key
  }

  private Object convert(Object value, Class targetClazz) {
    if (value == null) {
      return null;
    }
    BeanUtilsBean beanUtilsBean = new BeanUtilsBean(new ConvertUtilsBean() {
      @SneakyThrows
      @Override
      public Object convert(Object value, Class clazz) {
        if (clazz.isEnum()) {
          return Enum.valueOf(clazz, (String) value);
        } else if (clazz == Date.class) {
          String dateStr = ((String) value).replaceAll("-| |:", ""); // date format: 1, 2020-01-02 03:04:59, 2, 20200102030459
          dateStr = String.format("%-14s", dateStr).replaceAll(" ", "0");
          Date date = new SimpleDateFormat("yyyyMMddHHmmss").parse(dateStr);
          return date;
        } else {
          return super.convert(value, clazz);
        }
      }
    });
    return beanUtilsBean.getConvertUtils().convert(value, targetClazz);
  }

  /**
   * convert data to property
   */
  @SneakyThrows
  public Properties toProp() {
    Properties prop = new Properties();
    for (Field field : this.getClass().getDeclaredFields()) {
      Key keyAnn = field.getAnnotation(Key.class);
      if (keyAnn == null) {
        continue;
      }
      Object configValue = field.get(this);
      if (configValue == null) {
        continue;
      }
      prop.put(keyAnn.value(), configValue.toString());
    }
    return prop;
  }
}
