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
package org.apache.gobblin.metadata;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.gobblin.metadata.types.GlobalMetadata;


/**
 * This class collects metadata records, optionally merging them with a set of default metadata. It also
 * keeps track of all of the merged records so they can be published at a later date.
 */
public class GlobalMetadataCollector {
  public static final int UNLIMITED_SIZE = -1;

  private final LinkedHashSet<GlobalMetadata> metadataRecords;
  private final GlobalMetadata defaultMetadata;
  private final int cacheSize;

  private String lastSeenMetadataId;

  /**
   * Initialize a MetdataCollector with the given cache size.
   * @param cacheSize You can pass the value -1 to have an unlimited cache size.
   */
  public GlobalMetadataCollector(int cacheSize) {
    this(null, cacheSize);
  }

  /**
   * Initialize a MetadataCollector with some default metadata to merge incoming records with.
   * (Eg: a dataset-URN or a set of Transfer-Encodings).
   */
  public GlobalMetadataCollector(GlobalMetadata defaultMetadata, int cacheSize) {
    Preconditions.checkArgument(cacheSize == -1 || cacheSize > 0, "cacheSize must be -1 or greater than 0");

    this.defaultMetadata = defaultMetadata;
    this.cacheSize = cacheSize;
    this.lastSeenMetadataId = "";
    this.metadataRecords = new LinkedHashSet<>();
  }

  /**
   * Process a metadata record, merging it with default metadata.
   * <p>
   * If the combined (metadata + defaultMetadata) record is not present in the Collector's cache,
   * then the new metadata record will be stored in cache and returned. The oldest record in the cache will be evicted
   * if necessary.
   * <p>>
   * If the new record already exists in the cache, then the LRU time will be updated but this method will return null.
   */
  public synchronized GlobalMetadata processMetadata(GlobalMetadata metadata) {
    GlobalMetadata recordToAdd = getRecordToAdd(metadata);
    if (recordToAdd != null) {
      boolean isNew = addRecordAndEvictIfNecessary(recordToAdd);
      return isNew ? recordToAdd : null;
    }

    return null;
  }

  /**
   * Return a Set of all merged metadata records in the cache. The set is immutable.
   */
  public Set<GlobalMetadata> getMetadataRecords() {
    return Collections.unmodifiableSet(metadataRecords);
  }

  private boolean addRecordAndEvictIfNecessary(GlobalMetadata recordToAdd) {
    // First remove the element from the HashSet if it's already in there to reset
    // the 'LRU' piece; then add it back in
    boolean isNew = !metadataRecords.remove(recordToAdd);
    metadataRecords.add(recordToAdd);

    // Now remove the first element (which should be the oldest) from the list
    // if we've exceeded the cache size
    if (cacheSize != -1 && metadataRecords.size() > cacheSize) {
      Iterator<GlobalMetadata> recordIt = metadataRecords.iterator();
      recordIt.next(); // Remove the oldest element - don't care what it is
      recordIt.remove();
    }

    return isNew;
  }

  private GlobalMetadata getRecordToAdd(GlobalMetadata metadata) {
    if (metadata == null) {
      return defaultMetadata;
    }

    // Optimization - we know this record already has been seen, so don't
    // merge with defaults
    if (metadata.getId().equals(lastSeenMetadataId)) {
      return null;
    }

    lastSeenMetadataId = metadata.getId();
    if (defaultMetadata != null) {
      metadata.mergeWithDefaults(defaultMetadata);
    }
    return metadata;
  }
}
