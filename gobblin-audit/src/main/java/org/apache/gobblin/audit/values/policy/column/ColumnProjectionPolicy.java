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
package org.apache.gobblin.audit.values.policy.column;

import java.util.List;

import org.apache.avro.generic.GenericRecord;


/**
 * An interface that projects certain columns/fields of an input {@link GenericRecord} to generate a new {@link GenericRecord} that can be audited.
 * The field locations to project are an ordered string specifying the location of the nested field to retrieve.
 * For example, field1.nestedField1 takes the the value of the field "field1" of the record, and retrieves the field "nestedField1" from it
 */
public interface ColumnProjectionPolicy {

  /**
   * Get the key aka unique identifier fields to project in a {@link GenericRecord}
   */
  public List<String> getKeyColumnsToProject();

  /**
   * Get the delta fields to project in a {@link GenericRecord}. These are the fields used to determine changes over time.
   */
  public List<String> getDeltaColumnsToProject();

  /**
   * A union of {@link #getKeyColumnsToProject()} and {@link #getDeltaColumnsToProject()}
   */
  public List<String> getAllColumnsToProject();

  /**
   * Project key and delta columns/fields of the <code>inputRecord</code> and return a new {@link GenericRecord} with only the projected columns/fields
   * @param inputRecord the original record with all columns/fields
   * @return a new {@link GenericRecord} with only {@link #getAllColumnsToProject()}
   */
  public GenericRecord project(GenericRecord inputRecord);
}
