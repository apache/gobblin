/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction;

/**
 * An immutable class for managing Hive attributes.
 *
 * @author ziliu
 */
public final class HiveAttribute {
  private final String name;
  private final Type type;

  public enum Type {
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    TIMESTAMP,
    DATE,
    STRING,
    VARCHAR,
    CHAR,
    BOOLEAN,
    BINARY
  }

  private enum AvroType {
    BOOLEAN(Type.BOOLEAN),
    INT(Type.INT),
    LONG(Type.BIGINT),
    FLOAT(Type.FLOAT),
    DOUBLE(Type.DOUBLE),
    BYTES(Type.BINARY),
    STRING(Type.STRING),
    ENUM(Type.STRING);

    private final Type hiveType;

    private AvroType(Type hiveType) {
      this.hiveType = hiveType;
    }
  }

  public static Type fromAvroType(String avroTypeString) {
    try {
      AvroType.valueOf(avroTypeString.toUpperCase());
      return AvroType.valueOf(avroTypeString).hiveType;
    } catch (java.lang.RuntimeException e) {
      return null;
    }
  }

  public HiveAttribute(String name, Type type) {
    this.name = name;
    this.type = type;
  }

  public HiveAttribute(HiveAttribute attr) {
    this.name = attr.name;
    this.type = attr.type;
  }

  public String name() {
    return this.name;
  }

  public Type type() {
    return this.type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HiveAttribute)) {
      return false;
    }
    HiveAttribute other = (HiveAttribute) obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    return true;
  }
}
