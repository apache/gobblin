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

package org.apache.gobblin.azure.adf;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.tukaani.xz.UnsupportedOptionsException;

import java.io.IOException;
import java.util.List;


/**
 * An implementation of Gobblin source to run Azure Data Factory pipelines
 */
@Alpha
@Slf4j
public class ADFPipelineSource extends AbstractSource<JsonArray, JsonElement> {

  public ADFPipelineSource() {
    log.info("Hi");
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    log.info("State: " + state);
    MetricContext metricContext = Instrumented.getMetricContext(state, ADFPipelineSource.class);
    EventSubmitter eSubmitter =
        new EventSubmitter.Builder(metricContext, ADFPipelineSource.class.getSimpleName()).build();
    eSubmitter.submit("ADFPipelineSourceEvent");

    WorkUnit wu = WorkUnit.createEmpty();
    wu.addAll(state);
    TaskUtils.setTaskFactoryClass(wu, ADFExecutionTaskFactory.class);
    return Lists.newArrayList(wu);
  }

  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state) throws IOException {
    throw new UnsupportedOptionsException(String.format("Depending %s to create a %s instead of the default Task",
        ADFExecutionTaskFactory.class.getSimpleName(), AbstractADFPipelineExecutionTask.class.getSimpleName()));
  }

  @Override
  public void shutdown(SourceState state) {
    log.info("Show down " + ADFPipelineSource.class.getSimpleName());
  }
}
