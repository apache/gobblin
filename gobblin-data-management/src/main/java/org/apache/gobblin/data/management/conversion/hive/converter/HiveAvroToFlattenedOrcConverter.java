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
package org.apache.gobblin.data.management.conversion.hive.converter;

import org.apache.avro.Schema;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset.ConversionConfig;
import org.apache.gobblin.util.AvroFlattener;

/**
 * An Avro to ORC converter for avro to flattened ORC. {@link OrcFormats#FLATTENED_ORC}
 */
public class HiveAvroToFlattenedOrcConverter extends AbstractAvroToOrcConverter {

  private static AvroFlattener AVRO_FLATTENER = new AvroFlattener();

  /**
   * Flatten the <code>inputSchema</code>
   * {@inheritDoc}
   * @see org.apache.gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter#convertSchema(org.apache.avro.Schema, org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) {
    return AVRO_FLATTENER.flatten(inputSchema, false);
  }

  /**
   * Return true if flattened orc configurations are available. False otherwise
   * {@inheritDoc}
   * @see org.apache.gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter#hasConversionConfig()
   */
  @Override
  protected boolean hasConversionConfig() {
    return super.hiveDataset.getConversionConfigForFormat(OrcFormats.FLATTENED_ORC.getConfigPrefix()).isPresent();
  }

  @Override
  protected ConversionConfig getConversionConfig() {
    return super.hiveDataset.getConversionConfigForFormat(OrcFormats.FLATTENED_ORC.getConfigPrefix()).get();
  }
}
