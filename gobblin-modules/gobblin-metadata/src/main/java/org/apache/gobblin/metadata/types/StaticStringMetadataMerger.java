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
package org.apache.gobblin.metadata.types;

import org.apache.gobblin.metadata.MetadataMerger;
import org.apache.gobblin.writer.FsWriterMetrics;


/**
 * Metadata 'Merger' that is pre-provisioned with a static string and can not
 * actually merge data. This is to support backwards-compatible uses cases where a
 * user specifies metadata in job config and does _not_ want to publish
 * any other pipeline-produced metadata.
 */
public class StaticStringMetadataMerger implements MetadataMerger<String> {
  private final String metadata;

  public StaticStringMetadataMerger(String staticMetadata) {
    this.metadata = staticMetadata;
  }

  @Override
  public void update(String metadata) {
    /*
     * Since we don't know what format the string is in, we also don't know how to
     * merge anything.
     */
    throw new UnsupportedOperationException("Do not know how to merge with other strings!");
  }

  @Override
  public void update(FsWriterMetrics metrics) {
    throw new UnsupportedOperationException("Do not know how to merge FsWriterMetrics");
  }

  @Override
  public String getMergedMetadata() {
    return metadata;
  }
}
