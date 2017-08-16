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

import org.apache.commons.codec.digest.DigestUtils;

import gobblin.annotation.Alias;


/**
 * Replaces a {@link String} by it's hex Sha1 encoding. Takes no parameters.
 */
public class Sha1Replacer implements StringValueReplacer {

  @Alias(value = "sha1")
  public static class Factory implements ReplacerFactory {
    @Override
    public StringValueReplacer buildReplacer(String configurationValue, Properties properties) {
      return new Sha1Replacer();
    }
  }

  @Override
  public String replace(String originalValue) {
    return DigestUtils.sha1Hex(originalValue);
  }
}
