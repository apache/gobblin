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

package gobblin.util.io;

import java.net.URI;

import org.apache.hadoop.fs.LocalFileSystem;

import com.google.common.base.Joiner;

import gobblin.util.ClustersNames;
import gobblin.util.limiter.broker.SharedLimiterKey;

import lombok.Getter;


/**
 * A subclass of {@link SharedLimiterKey} with more detailed information on the source and target of
 * a throttled {@link StreamCopier}.
 */
@Getter
public class StreamCopierSharedLimiterKey extends SharedLimiterKey {

  public static final String STREAM_COPIER_KEY_PREFIX = "streamCopierBandwidth";

  private final URI sourceURI;
  private final URI targetURI;

  public StreamCopierSharedLimiterKey(URI sourceURI, URI targetURI) {
    super(getLimitedResourceId(sourceURI, targetURI));
    this.sourceURI = sourceURI;
    this.targetURI = targetURI;
  }

  private static String getLimitedResourceId(URI sourceURI, URI targetURI) {
    return Joiner.on(".").join(STREAM_COPIER_KEY_PREFIX, getFSIdentifier(sourceURI), getFSIdentifier(targetURI));
  }

  private static String getFSIdentifier(URI uri) {
    if (new LocalFileSystem().getScheme().equals(uri.getScheme())) {
      return "localhost";
    } else {
      return ClustersNames.getInstance().getClusterName(uri.toString());
    }
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

    StreamCopierSharedLimiterKey that = (StreamCopierSharedLimiterKey) o;

    if (!sourceURI.equals(that.sourceURI)) {
      return false;
    }
    return targetURI.equals(that.targetURI);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + sourceURI.hashCode();
    result = 31 * result + targetURI.hashCode();
    return result;
  }
}
