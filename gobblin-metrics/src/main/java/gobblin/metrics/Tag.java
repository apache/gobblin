/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.util.AbstractMap;
import java.util.Map;


/**
 * A class representing a dimension or property associated with a {@link Taggable}.
 *
 * @author ynli
 */
public class Tag<T> extends AbstractMap.SimpleEntry<String, T> {

  public Tag(String key, T value) {
    super(key, value);
  }

  public Tag(Map.Entry<? extends String, ? extends T> entry) {
    super(entry);
  }

  @Override
  public String toString() {
    return getKey() + ":" + getValue();
  }
}
