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


/**
 * Replaces a {@link String} value for a different {@link String}. Used in {@link AvroFieldReplacer}.
 */
public interface StringValueReplacer {

  /**
   * Builds a {@link StringValueReplacer}.
   */
  interface ReplacerFactory {
    /**
     * Build a {@link StringValueReplacer}.
     * @param configurationValue A parameter passed in by the user for this replacer. If the user did not pass a parameter,
     *                           use "" instead.
     * @param properties The full job properties.
     * @return a {@link StringValueReplacer}.
     */
    StringValueReplacer buildReplacer(String configurationValue, Properties properties);
  }

  /**
   * Compute the replacement for the input string.
   */
  String replace(String originalValue);
}
