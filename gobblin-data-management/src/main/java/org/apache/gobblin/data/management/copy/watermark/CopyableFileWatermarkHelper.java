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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.source.extractor.WatermarkInterval;


/**
 * Helper class for {@link CopyableFile} based watermark.
 */
public class CopyableFileWatermarkHelper {
  /**
   * Watermark creator for workunits created from CopyEntities.
   */
  public static final String WATERMARK_CREATOR = CopyConfiguration.COPY_PREFIX + ".watermarkCreator";

  /**
   * Get Optional {@link CopyableFileWatermarkGenerator} from {@link State}.
   */
  public static Optional<CopyableFileWatermarkGenerator> getCopyableFileWatermarkGenerator(State state)
      throws IOException {
    try {
      if (state.contains(WATERMARK_CREATOR)) {
        Class<?> watermarkCreatorClass = Class.forName(state.getProp(WATERMARK_CREATOR));
        return Optional.of((CopyableFileWatermarkGenerator) watermarkCreatorClass.newInstance());
      } else {
        return Optional.absent();
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new IOException("Failed to instantiate watermarkCreator.");
    }
  }

  /**
   * Return Optional {@link WatermarkInterval} for {@link CopyableFile} using {@link CopyableFileWatermarkGenerator}.
   */
  public static Optional<WatermarkInterval> getCopyableFileWatermark(CopyableFile copyableFile,
      Optional<CopyableFileWatermarkGenerator> watermarkGenerator)
      throws IOException {
    if (!watermarkGenerator.isPresent()) {
      return Optional.absent();
    }
    return watermarkGenerator.get().generateWatermarkIntervalForCopyableFile(copyableFile);
  }
}
