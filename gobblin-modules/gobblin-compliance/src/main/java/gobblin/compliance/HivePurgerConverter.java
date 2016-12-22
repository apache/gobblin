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
 * A {@link Converter} to build compliance queries. Queries are added to the {@link ComplianceRecord}.
 *
 * @author adsharma
 */
public class HivePurgerConverter extends Converter<Class<?>, Class<?>, ComplianceRecord, ComplianceRecord> {

  @Override
  public Class<?> convertSchema(Class<?> schema, WorkUnitState state) {
    return schema;
  }

  @Override
  public Iterable<ComplianceRecord> convertRecord(Class<?> schema, ComplianceRecord record,
      WorkUnitState state) {
    record.setPurgeQueries(HivePurgerQueryTemplate.getPurgeQueries(record));
    return new SingleRecordIterable<>(record);
  }
}

