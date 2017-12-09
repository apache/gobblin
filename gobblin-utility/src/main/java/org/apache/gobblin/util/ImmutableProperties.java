/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.util;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import lombok.experimental.Delegate;


/**
 * Immutable wrapper for {@link Properties}.
 */
public class ImmutableProperties extends Properties {
  @Delegate
  private final Map<Object, Object> props;

  public ImmutableProperties(Properties props) {
    this.props = Collections.unmodifiableMap(props);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ImmutableProperties that = (ImmutableProperties) o;

    return props != null ? props.equals(that.props) : that.props == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (props != null ? props.hashCode() : 0);
    return result;
  }

  /**
   * Need to override this method otherwise it will call super.get(key).
   */
  @Override
  public String getProperty(String key) {
    Object oval = this.get(key);
    String sval = (oval instanceof String) ? (String) oval : null;
    return ((sval == null) && (defaults != null)) ? defaults.getProperty(key) : sval;
  }
}
