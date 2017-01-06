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

package gobblin;

import gobblin.converter.Converter;
import gobblin.fork.ForkOperator;
import gobblin.publisher.DataPublisher;
import gobblin.qualitychecker.row.RowLevelPolicy;
import gobblin.qualitychecker.task.TaskLevelPolicy;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.writer.DataWriter;


/**
 * Enumeration of available gobblin constructs.
 */
public enum Constructs {

  /** {@link gobblin.source.Source} */
  SOURCE("Source", Source.class),
  /** {@link gobblin.source.extractor.Extractor} */
  EXTRACTOR("Extractor", Extractor.class),
  /** {@link gobblin.converter.Converter} */
  CONVERTER("Converter", Converter.class),
  /** {@link gobblin.qualitychecker.row.RowLevelPolicy} */
  ROW_QUALITY_CHECKER("RowLevelPolicy", RowLevelPolicy.class),
  /** {@link gobblin.qualitychecker.task.TaskLevelPolicy} */
  TASK_QUALITY_CHECKER("TaskLevelPolicy", TaskLevelPolicy.class),
  /** {@link gobblin.fork.ForkOperator} */
  FORK_OPERATOR("ForkOperator", ForkOperator.class),
  /** {@link gobblin.writer.DataWriter} */
  WRITER("DataWriter", DataWriter.class),
  /** {@link gobblin.publisher.DataPublisher} */
  DATA_PUBLISHER("DataPublisher",DataPublisher.class);

  private final String name;
  private final Class<?> klazz;

  Constructs(String name, Class<?> klazz) {
    this.name = name;
    this.klazz = klazz;
  }

  @Override
  public String toString() {
    return this.name;
  }

  public Class<?> constructClass() {
    return this.klazz;
  }
}
