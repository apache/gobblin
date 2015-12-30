/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;

import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Preconditions;

/**
 * A simple implementation of {@link ConfigKeyPath} using a singly-linked list of path
 * components.
 */
public class LinkedListConfigKeyPath implements ConfigKeyPath {
  private static final LinkedListConfigKeyPath ROOT_NODE = new LinkedListConfigKeyPath("", null);

  private final String ownPathName;
  private final LinkedListConfigKeyPath parent;

  private LinkedListConfigKeyPath(String ownPathName, LinkedListConfigKeyPath parent) {
    Preconditions.checkNotNull(ownPathName);
    this.ownPathName = ownPathName;
    this.parent = parent;
  }

  /**
   * Creates a new LinkedListConfigKeyPath objects from a path string. The path string must start
   * with a "/".
   *
   * @param configPathString      the config path string to parse
   * @return LinkedListConfigKeyPath corresponding to the path string
   * @throws IllegalArgumentException if the path does not start with a "/"
   */
  public static LinkedListConfigKeyPath createFromPathString(String configPathString) {
    if (!configPathString.startsWith("/")) {
        throw new IllegalArgumentException("Path must start with /: " + configPathString);
    }
    String[] components = configPathString.split("/");
    LinkedListConfigKeyPath res = ROOT_NODE;
    for (int i = 1; i < components.length; ++i) {
      if (components[i].isEmpty()) continue;
      res = (LinkedListConfigKeyPath)res.createChild(components[i]);
    }
    return res;
  }

  /**
   * Creates a new LinkedListConfigKeyPath objects from a config URI. The method will string the
   * scheme and authority. The remaining path string must start with a "/".
   *
   * @param configURI      the config URI to parse
   * @return LinkedListConfigKeyPath corresponding to the path in the config URI
   * @throws IllegalArgumentException if the path does not start with a "/"
   */
  public static LinkedListConfigKeyPath createFromURI(URI configURI) {
    String pathString = configURI.getPath();
    if (null == pathString || pathString.isEmpty()) {
      pathString = "/";
    }
    return createFromPathString(pathString);
  }

  /**
   * Creates a new LinkedListConfigKeyPath objects from a config URI string. The method will string the
   * scheme and authority. The remaining path string must start with a "/".
   *
   * @param configURI      the config URI string to parse
   * @return LinkedListConfigKeyPath corresponding to the path in the config URI
   * @throws IllegalArgumentException if the path does not start with a "/"
   */
  public static LinkedListConfigKeyPath createFromURIString(String configURI) {
    try {
      return createFromURI(new URI(configURI));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConfigKeyPath getParent() {
    if (!isRootPath()) {
      return this.parent;
    }
    throw new UnsupportedOperationException("No parent for root");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOwnPathName() {
    return this.ownPathName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConfigKeyPath createChild(String childPathName) {
    Preconditions.checkNotNull(childPathName);
    Preconditions.checkArgument(!childPathName.isEmpty());
    return new LinkedListConfigKeyPath(childPathName, this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getAbsolutePathString() {
    if (isRootPath()) {
      return "/";
    }
    StringBuilder res = new StringBuilder(128);
    constructAbsolutePathString(res);
    return res.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isRootPath() {
    return this == ROOT_NODE;
  }

  public static ConfigKeyPath getRootPath() {
    return ROOT_NODE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((ownPathName == null) ? 0 : ownPathName.hashCode());
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
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
    LinkedListConfigKeyPath other = (LinkedListConfigKeyPath) obj;
    if (ownPathName == null) {
      if (other.ownPathName != null)
        return false;
    } else if (!ownPathName.equals(other.ownPathName))
      return false;
    if (parent == null) {
      if (other.parent != null)
        return false;
    } else if (!parent.equals(other.parent))
      return false;
    return true;
  }

  private void constructAbsolutePathString(StringBuilder pathBuilder) {
    if (isRootPath()) {
      return;
    }
    this.parent.constructAbsolutePathString(pathBuilder);
    pathBuilder.append('/');
    pathBuilder.append(this.ownPathName);
  }
}
