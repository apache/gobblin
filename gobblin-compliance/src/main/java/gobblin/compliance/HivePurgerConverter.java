/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.compliance;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.SingleRecordIterable;


/**
 * This class is used to add queries to the {@link HivePurgerPartitionRecord}, which will be further passed to the writer to execute them.
 *
 * @author adsharma
 */
public class HivePurgerConverter extends Converter<HivePurgerPartitionRecordSchema, HivePurgerPartitionRecordSchema, HivePurgerPartitionRecord, HivePurgerPartitionRecord> {

  @Override
  public HivePurgerPartitionRecordSchema convertSchema(HivePurgerPartitionRecordSchema schema, WorkUnitState state) {
    return schema;
  }

  @Override
  public Iterable<HivePurgerPartitionRecord> convertRecord(HivePurgerPartitionRecordSchema schema,
      HivePurgerPartitionRecord record, WorkUnitState state) {
    record.addPurgeQueries(HivePurgerQueryTemplate.getPurgeQueries(record));
    return new SingleRecordIterable<>(record);
  }
}

