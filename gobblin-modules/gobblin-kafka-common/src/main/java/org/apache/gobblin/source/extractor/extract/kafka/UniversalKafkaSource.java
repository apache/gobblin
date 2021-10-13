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

package org.apache.gobblin.source.extractor.extract.kafka;

import com.google.common.eventbus.EventBus;
import java.io.IOException;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.InfiniteSource;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.EventBasedExtractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.stream.WorkUnitChangeEvent;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import com.google.common.base.Preconditions;


/**
 * A {@link KafkaSource} to use with arbitrary {@link EventBasedExtractor}. Specify the extractor to use with key
 * {@link #EXTRACTOR_TYPE}.
 */
@Slf4j
public class UniversalKafkaSource<S, D> extends KafkaSource<S, D> implements InfiniteSource<S, D> {

  public static final String EXTRACTOR_TYPE = "gobblin.source.kafka.extractorType";
  private final EventBus eventBus = new EventBus(this.getClass().getSimpleName());

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state)
      throws IOException {
    Preconditions.checkArgument(state.contains(EXTRACTOR_TYPE), "Missing key " + EXTRACTOR_TYPE);

    try {
      ClassAliasResolver<EventBasedExtractor> aliasResolver = new ClassAliasResolver<>(EventBasedExtractor.class);
      Class<? extends EventBasedExtractor> klazz = aliasResolver.resolveClass(state.getProp(EXTRACTOR_TYPE));

      return GobblinConstructorUtils.invokeLongestConstructor(klazz, state);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public void onWorkUnitUpdate(List<String> oldTaskIds, List<WorkUnit> newWorkUnits) {
    if (this.eventBus != null) {
      log.info("post workunit change event");
      this.eventBus.post(new WorkUnitChangeEvent(oldTaskIds, newWorkUnits));
    }
  }

  @Override
  public EventBus getEventBus() {
    return this.eventBus;
  }
}
