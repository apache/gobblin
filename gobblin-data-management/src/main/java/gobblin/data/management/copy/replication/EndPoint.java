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

package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileStatus;

import com.google.common.base.Optional;

import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.Watermark;


/**
 * Used to encapsulate all the information of a replica, including original source, during replication process.
 *
 * <ul>
 *  <li>Configuration of the data reside on this replica
 *  <li>Whether the replica is original source
 *  <li>The {@link Watermark} of this replica
 * </ul>
 * @author mitu
 *
 */
public interface EndPoint {

  /**
   * @return true iff this represents the original source
   */
  public boolean isSource();

  /**
   * @return the end point name
   */
  public String getEndPointName();

  /**
   *
   * @return the {@link Watermark} of the replica
   */
  public Optional<ComparableWatermark> getWatermark();

  /**
   * @return whether this {@link EndPoint} is available to replica data
   */
  public boolean isFileSystemAvailable();

  /**
   *
   * @return all the {@link FileStatus}s of this {@link EndPoint} in the context of data replication
   */
  public Collection<FileStatus> getFiles() throws IOException;
}
