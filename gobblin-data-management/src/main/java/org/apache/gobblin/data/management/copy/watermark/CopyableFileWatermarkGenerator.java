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

package gobblin.data.management.copy.watermark;

import java.io.IOException;

import com.google.common.base.Optional;

import gobblin.data.management.copy.CopyableFile;
import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.WatermarkInterval;


/**
 * Watermark generator for {@link CopyableFile}.
 */
public interface CopyableFileWatermarkGenerator {
  /**
   * Generate optional {@link WatermarkInterval} for a given {@link CopyableFile}.
   */
  public Optional<WatermarkInterval> generateWatermarkIntervalForCopyableFile(CopyableFile copyableFile) throws IOException;

  /**
   * @return the implemention class of {@link ComparableWatermark}. It needs to be comparable for tracking/filtering {@link CopyableFile}s.
   */
  public Class<? extends ComparableWatermark> getWatermarkClass();
}
