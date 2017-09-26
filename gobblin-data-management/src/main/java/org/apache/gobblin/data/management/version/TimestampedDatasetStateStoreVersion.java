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

package org.apache.gobblin.data.management.version;

import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.joda.time.DateTime;
import lombok.Getter;

/**
 * {@link TimestampedDatasetVersion} that has a {@link DatasetStateStoreEntryManager}
 */
@Getter
public class TimestampedDatasetStateStoreVersion extends TimestampedDatasetVersion implements DatasetStateStoreVersion {

  private final DatasetStateStoreEntryManager entry;

  public TimestampedDatasetStateStoreVersion(DatasetStateStoreEntryManager entry) {
    super(new DateTime(entry.getTimestamp()), null);
    this.entry = entry;
  }

  @Override
  public int compareTo(FileSystemDatasetVersion other) {
    TimestampedDatasetVersion otherAsDateTime = (TimestampedDatasetVersion) other;
    return this.version.equals(otherAsDateTime.version) ? 0 : this.version.compareTo(otherAsDateTime.version);
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return this.version.hashCode();
  }
}
