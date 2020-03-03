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
      fulFillFields(this.getClass(), prop);
      afterFulfill();
    }
  }

  /**
   * subclass could do something after fulfill by override afterFulfill
   */
  protected void afterFulfill() {
  }

  @SneakyThrows
  private void fulFillFields(Class<? extends TypedConfig> clazz, Properties prop) {
    if (!clazz.equals(TypedConfig.class)) {
      this.fulFillFields((Class<? extends TypedConfig>) clazz.getSuperclass(), prop);
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
      if (configValue == null) {
        continue;
      }
      configValue = ConstraintUtil.constraint(field, configValue, defaultValue);
      field.set(this, convert(configValue, field.getType()));
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
          String dateStr = ((String) value).replaceAll("-| |:", "");
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
   * subclass could do something before toProp by overriding
   */
  protected void beforeToProp() {
  }

  /**
   * convert data to property
   */
  @SneakyThrows
  public Properties toProp() {
    beforeToProp();
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

