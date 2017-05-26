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

package gobblin.util.filesystem;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Strings;

import gobblin.broker.iface.SharedResourceKey;

import lombok.Getter;


@Getter
public class FileSystemKey implements SharedResourceKey {

  private final URI uri;
  private final Configuration configuration;

  public FileSystemKey(URI uri, Configuration configuration) {
    this.configuration = configuration;
    this.uri = resolveURI(uri, configuration);
  }

  private URI resolveURI(URI uri, Configuration configuration) {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null && authority == null) {     // use default FS
      return FileSystem.getDefaultUri(configuration);
    }

    if (scheme != null && authority == null) {     // no authority
      URI defaultUri = FileSystem.getDefaultUri(configuration);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return defaultUri;                         // return default
      }
    }

    try {
      return new URI(scheme, Strings.nullToEmpty(authority), "/", null, null);
    } catch (URISyntaxException use) {
      // This should never happen
      throw new RuntimeException(use);
    }
  }

  @Override
  public String toConfigurationKey() {
    return this.uri.getScheme() + (this.uri.getHost() == null ? "" : ("." + this.uri.getHost()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FileSystemKey that = (FileSystemKey) o;

    return uri == null ? that.uri == null : uri.equals(that.uri);
  }

  @Override
  public int hashCode() {
    return uri != null ? uri.hashCode() : 0;
  }
}
