package org.apache.gobblin.test.generator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.gobblin.test.InMemoryFormat;


@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RegisteredValueGeneratorFactory {
  // The in-memory format that this value generator factory serves
  InMemoryFormat inMemoryFormat();
}

