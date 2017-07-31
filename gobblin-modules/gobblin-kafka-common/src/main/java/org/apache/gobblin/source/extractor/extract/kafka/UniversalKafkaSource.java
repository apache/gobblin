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

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A {@link KafkaSource} to use with arbitrary {@link KafkaExtractor}. Specify the extractor to use with key
 * {@link #EXTRACTOR_TYPE}.
 */
public class UniversalKafkaSource<S, D> extends KafkaSource<S, D> {

  public static final String EXTRACTOR_TYPE = "gobblin.source.kafka.extractorType";

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
    Preconditions.checkArgument(state.contains(EXTRACTOR_TYPE), "Missing key " + EXTRACTOR_TYPE);

    try {
      ClassAliasResolver<KafkaExtractor> aliasResolver = new ClassAliasResolver<>(KafkaExtractor.class);
      Class<? extends KafkaExtractor> klazz = aliasResolver.resolveClass(state.getProp(EXTRACTOR_TYPE));

      return GobblinConstructorUtils.invokeLongestConstructor(klazz, state);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
