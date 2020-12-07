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
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    if (obj instanceof TimestampedDatasetStateStoreVersion) {
      TimestampedDatasetStateStoreVersion other = (TimestampedDatasetStateStoreVersion)obj;

      if (this.entry.equals(other.getEntry())) {
        return super.equals(obj);
      }
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = this.version.hashCode();
    result = 31 * result + (entry != null ? entry.hashCode() : 0);
    return result;
  }
}
