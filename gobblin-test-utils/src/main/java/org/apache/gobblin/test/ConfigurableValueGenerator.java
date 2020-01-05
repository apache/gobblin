package org.apache.gobblin.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.typesafe.config.Config;

import org.apache.gobblin.test.type.Type;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigurableValueGenerator {
  String name();
  Type[] targetTypes();
  Class configuration() default Config.class;
}
