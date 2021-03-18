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
  // The name that you want to register this generator under
  String name();
  // The logical types that this generator can generate
  Type[] targetTypes();
  // The configClass class that this class's constructor expects
  Class configClass() default Config.class;
  // The physical in-memory format that this generator generates
  InMemoryFormat targetFormat();
}
