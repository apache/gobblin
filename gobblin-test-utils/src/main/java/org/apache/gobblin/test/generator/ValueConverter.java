package org.apache.gobblin.test.generator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.type.Type;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ValueConverter {
    // The in-memory type that this converter returns
    InMemoryFormat inMemoryType();
    // The logical types that this converter converts from
    Type[] logicalTypes();
}
