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

package gobblin.data.management.version;

import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import com.google.common.collect.Sets;

import lombok.Getter;


/**
 * {@link gobblin.data.management.version.DatasetVersion} based on a timestamp.
 */
@Getter
public class TimestampedDatasetVersion implements FileSystemDatasetVersion {
  protected final DateTime version;
  protected final Path path;

  public TimestampedDatasetVersion(DateTime version, Path path) {
    this.version = version;
    this.path = path;
  }

  public DateTime getDateTime() {
    return this.version;
  }

  @Override
  public int compareTo(FileSystemDatasetVersion other) {
    TimestampedDatasetVersion otherAsDateTime = (TimestampedDatasetVersion) other;
    return this.version.equals(otherAsDateTime.version) ? this.path.compareTo(otherAsDateTime.path)
        : this.version.compareTo(otherAsDateTime.version);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TimestampedDatasetVersion && compareTo((TimestampedDatasetVersion) obj) == 0;
  }

  @Override
  public int hashCode() {
    return this.version.hashCode() + this.path.hashCode();
  }

  @Override
  public String toString() {
    return "Version " + this.version.toString(DateTimeFormat.shortDateTime()) + " at path " + this.path;
  }

  @Override
  public Set<Path> getPaths() {
    return Sets.newHashSet(this.path);
  }
}
