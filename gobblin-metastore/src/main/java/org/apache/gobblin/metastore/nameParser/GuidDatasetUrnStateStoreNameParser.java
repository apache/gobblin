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

package org.apache.gobblin.metastore.nameParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.io.LineReader;

import org.apache.gobblin.util.guid.Guid;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Implements {@link DatasetUrnStateStoreNameParser} using {@link Guid}.
 */
public class GuidDatasetUrnStateStoreNameParser implements DatasetUrnStateStoreNameParser {
  private static final String TMP_SUFFIX = "_tmp";

  private final FileSystem fs;
  private Path versionIdentifier;

  @AllArgsConstructor
  public enum StateStoreNameVersion {
    /**
     * DatasetUrn is directly used as the state store name.
     * This is the initial status when {@link GuidDatasetUrnStateStoreNameParser} is first enabled.
     * It will be migrated to {@link StateStoreNameVersion#V1}.
     */
    V0(StringUtils.EMPTY),
    /**
     * DatasetUrn is hashed into shorten name.
     */
    V1("datasetUrnNameMapV1");

    @Getter
    private String datasetUrnNameMapFile;
  }

  private final StateStoreNameVersion version;
  @VisibleForTesting
  protected final BiMap<String, String> sanitizedNameToDatasetURNMap;

  public GuidDatasetUrnStateStoreNameParser(FileSystem fs, Path jobStatestoreRootDir)
      throws IOException {
    this.fs = fs;
    this.sanitizedNameToDatasetURNMap = Maps.synchronizedBiMap(HashBiMap.<String, String>create());
    this.versionIdentifier = new Path(jobStatestoreRootDir, StateStoreNameVersion.V1.getDatasetUrnNameMapFile());
    if (this.fs.exists(versionIdentifier)) {
      this.version = StateStoreNameVersion.V1;
      try (InputStream in = this.fs.open(versionIdentifier)) {
        LineReader lineReader = new LineReader(new InputStreamReader(in, Charsets.UTF_8));
        String shortenName = lineReader.readLine();
        while (shortenName != null) {
          String datasetUrn = lineReader.readLine();
          this.sanitizedNameToDatasetURNMap.put(shortenName, datasetUrn);
          shortenName = lineReader.readLine();
        }
      }
    } else {
      this.version = StateStoreNameVersion.V0;
    }
  }

  /**
   * Get datasetUrn for the given state store name.
   * If the state store name can be found in {@link #sanitizedNameToDatasetURNMap}, the original datasetUrn will be returned.
   * Otherwise, the state store name will be returned.
   */
  @Override
  public String getDatasetUrnFromStateStoreName(String stateStoreName)
      throws IOException {
    if (version == StateStoreNameVersion.V0 || !this.sanitizedNameToDatasetURNMap.containsKey(stateStoreName)) {
      return stateStoreName;
    } else {
      return this.sanitizedNameToDatasetURNMap.get(stateStoreName);
    }
  }

  /**
   * Merge the given {@link Collection} of datasetUrns with {@link #sanitizedNameToDatasetURNMap}, and write the results
   * to the {@link Path} of {@link #versionIdentifier}, which is a text file.
   */
  @Override
  public void persistDatasetUrns(Collection<String> datasetUrns)
      throws IOException {
    for (String datasetUrn : datasetUrns) {
      String key = Guid.fromStrings(datasetUrn).toString();
      if (!this.sanitizedNameToDatasetURNMap.containsKey(key)) {
        this.sanitizedNameToDatasetURNMap.put(key, datasetUrn);
      } else if (!this.sanitizedNameToDatasetURNMap.get(key).equals(datasetUrn)) {
        // This should not happen for datasetUrns since Guid is SHA1 based...
        throw new RuntimeException(
            "Found a collision for " + datasetUrn + " with existing: " + this.sanitizedNameToDatasetURNMap.get(key));
      }
    }
    Path tmpMapFile = new Path(this.versionIdentifier.getParent(), this.versionIdentifier.getName() + TMP_SUFFIX);
    try (FSDataOutputStream fsout = this.fs.create(tmpMapFile)) {
      for (String key : this.sanitizedNameToDatasetURNMap.keySet()) {
        fsout.write(key.getBytes(Charsets.UTF_8));
        fsout.writeByte('\n');
        fsout.write(this.sanitizedNameToDatasetURNMap.get(key).getBytes(Charsets.UTF_8));
        fsout.writeByte('\n');
      }
    }
    // Back up the previous file first, and then rename the new file.
    if (this.fs.exists(this.versionIdentifier) && !this.fs.rename(this.versionIdentifier,
        new Path(this.versionIdentifier.getParent(),
            this.versionIdentifier.getName() + "_" + System.currentTimeMillis()))) {
      throw new IOException(
          "Failed to back up existing datasetUrn to stateStore name mapping file: " + this.versionIdentifier);
    }

    if (!fs.rename(tmpMapFile, this.versionIdentifier)) {
      throw new IOException("Failed to rename from " + tmpMapFile + " to " + this.versionIdentifier);
    }
  }

  @Override
  public String getStateStoreNameFromDatasetUrn(String datasetUrn)
      throws IOException {
    if (!this.sanitizedNameToDatasetURNMap.inverse().containsKey(datasetUrn)) {
      String guid = Guid.fromStrings(datasetUrn).toString();
      this.sanitizedNameToDatasetURNMap.put(guid, datasetUrn);
    }
    return this.sanitizedNameToDatasetURNMap.inverse().get(datasetUrn);
  }
}
