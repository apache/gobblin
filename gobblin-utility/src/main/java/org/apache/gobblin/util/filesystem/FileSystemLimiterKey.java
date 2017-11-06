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

package org.apache.gobblin.util.filesystem;

import java.net.URI;

import com.google.common.base.Strings;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.limiter.broker.SharedLimiterKey;

/**
 * {@link SharedLimiterKey} used for NameNode throttling.
 */
public class FileSystemLimiterKey extends SharedLimiterKey {

  public static final String RESOURCE_LIMITED_PREFIX = "filesystem";
  private final URI uri;
  public final String serviceName;

  public FileSystemLimiterKey(URI uri) {
    this(uri, null);
  }

  public FileSystemLimiterKey(URI uri, String serviceName) {
    super(RESOURCE_LIMITED_PREFIX + "/" + getFSIdentifier(uri) + (Strings.isNullOrEmpty(serviceName) ? "" : "/" + serviceName));
    this.uri = uri;
    this.serviceName = serviceName;
  }

  private static String getFSIdentifier(URI uri) {
    return ClustersNames.getInstance().getClusterName(uri.toString());
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

    FileSystemLimiterKey that = (FileSystemLimiterKey) o;

    return uri.equals(that.uri);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + uri.hashCode();
    return result;
  }
}
