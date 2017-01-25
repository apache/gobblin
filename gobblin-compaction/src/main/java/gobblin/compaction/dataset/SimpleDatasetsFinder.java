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

package gobblin.compaction.dataset;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;


/**
 * Implementation of {@link DatasetsFinder}. It simply takes {@link MRCompactor#COMPACTION_INPUT_DIR} as input,
 * and {@link MRCompactor#COMPACTION_DEST_DIR} as output.
 */
public class SimpleDatasetsFinder extends DatasetsFinder {

  public SimpleDatasetsFinder(State state) {
    super(state);
  }

  /**
   * Create a dataset using {@link #inputDir} and {@link #destDir}.
   * Set dataset input path to be {@link #destDir} if {@link #recompactDatasets} is true.
   */
  @Override
  public Set<Dataset> findDistinctDatasets() throws IOException {
    Set<Dataset> datasets = Sets.newHashSet();
    Path inputPath = new Path(this.inputDir);
    Path inputLatePath = new Path(inputPath, MRCompactor.COMPACTION_LATE_DIR_SUFFIX);
    Path outputPath = new Path(this.destDir);
    Path outputLatePath = new Path(outputPath, MRCompactor.COMPACTION_LATE_DIR_SUFFIX);
    Dataset dataset =
        new Dataset.Builder().withPriority(this.getDatasetPriority(inputPath.getName()))
            .addInputPath(this.recompactDatasets ? outputPath : inputPath)
            .addInputLatePath(this.recompactDatasets ? outputLatePath : inputLatePath).withOutputPath(outputPath)
            .withOutputLatePath(outputLatePath).withOutputTmpPath(new Path(this.tmpOutputDir)).build();
    datasets.add(dataset);
    return datasets;
  }
}
