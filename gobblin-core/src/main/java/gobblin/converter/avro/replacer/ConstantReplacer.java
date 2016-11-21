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

package gobblin.converter.avro.replacer;

import java.util.Properties;

import gobblin.annotation.Alias;

import lombok.AllArgsConstructor;


/**
 * Replaces strings by a constant value (passed in as parameter in the configuration key {@link AvroFieldReplacer#REPLACE_FIELD_KEY}.
 *
 * Usage: {@link AvroFieldReplacer#REPLACE_FIELD_KEY}.<my-field> = constant:<replacement-value>
 */
@AllArgsConstructor
public class ConstantReplacer implements StringValueReplacer {

  @Alias(value = "constant")
  public static class Factory implements ReplacerFactory {
    @Override
    public StringValueReplacer buildReplacer(String configurationValue, Properties properties) {
      return new ConstantReplacer(configurationValue);
    }
  }

  private final String replacement;

  @Override
  public String replace(String originalValue) {
    return replacement;
  }
}
