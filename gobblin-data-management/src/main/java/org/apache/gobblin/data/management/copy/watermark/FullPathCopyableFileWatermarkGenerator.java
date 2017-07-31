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

package org.apache.gobblin.data.management.copy.watermark;

import java.io.IOException;

import com.google.common.base.Optional;

import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.source.extractor.ComparableWatermark;
import org.apache.gobblin.source.extractor.WatermarkInterval;


/**
 * Implementation of {@link CopyableFileWatermarkGenerator} that generates {@link StringWatermark} based on {@link CopyableFile}'s full path.
 */
public class FullPathCopyableFileWatermarkGenerator implements CopyableFileWatermarkGenerator {

  @Override
  public Optional<WatermarkInterval> generateWatermarkIntervalForCopyableFile(CopyableFile copyableFile)
      throws IOException {
    StringWatermark stringWatermark = new StringWatermark(copyableFile.getFileStatus().getPath().toString());
    return Optional.of(new WatermarkInterval(stringWatermark, stringWatermark));
  }

  @Override
  public Class<? extends ComparableWatermark> getWatermarkClass() {
    return StringWatermark.class;
  }
}
