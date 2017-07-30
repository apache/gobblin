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

package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;


public class SingleLinkedListConfigKeyPath implements ConfigKeyPath {

  public static final String PATH_DELIMETER = "/";
  public static final SingleLinkedListConfigKeyPath ROOT = new SingleLinkedListConfigKeyPath(null, "");

  private final ConfigKeyPath parent;
  private final String ownName;

  // constructor private, can only create path from ROOT using createChild method
  private SingleLinkedListConfigKeyPath(ConfigKeyPath parent, String name) {
    this.parent = parent;
    this.ownName = name;
  }

  @Override
  public ConfigKeyPath getParent() {
    if (this.isRootPath())
      throw new UnsupportedOperationException("Can not getParent from Root");

    return this.parent;
  }

  @Override
  public String getOwnPathName() {
    return this.ownName;
  }

  @Override
  public ConfigKeyPath createChild(String childPathName) {
    if (childPathName == null || childPathName.length() == 0 || childPathName.indexOf(PATH_DELIMETER) >= 0) {
      throw new IllegalArgumentException(
          String.format("Name \"%s\" can not be null/empty string and can not contains the delimiter \"%s\"",
              childPathName, PATH_DELIMETER));
    }
    return new SingleLinkedListConfigKeyPath(this, childPathName);
  }

  @Override
  public String getAbsolutePathString() {
    if (this.isRootPath()) {
      return this.getOwnPathName() + PATH_DELIMETER;
    }

    // first level children do not need to add "/"
    if (this.parent.isRootPath())
      return this.parent.getAbsolutePathString() + this.ownName;

    return this.parent.getAbsolutePathString() + PATH_DELIMETER + this.ownName;
  }

  @Override
  public boolean isRootPath() {
    return this == ROOT;
  }

  @Override
  public String toString() {
    return this.getAbsolutePathString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.ownName == null) ? 0 : this.ownName.hashCode());
    result = prime * result + ((this.parent == null) ? 0 : this.parent.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SingleLinkedListConfigKeyPath other = (SingleLinkedListConfigKeyPath) obj;
    if (this.ownName == null) {
      if (other.ownName != null)
        return false;
    } else if (!this.ownName.equals(other.ownName))
      return false;
    if (this.parent == null) {
      if (other.parent != null)
        return false;
    } else if (!this.parent.equals(other.parent))
      return false;
    return true;
  }
}
