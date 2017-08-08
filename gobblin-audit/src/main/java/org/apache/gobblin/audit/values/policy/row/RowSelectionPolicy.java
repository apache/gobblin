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
package org.apache.gobblin.audit.values.policy.row;

import org.apache.avro.generic.GenericRecord;

/**
 * An interface to decide if a row needs to be audited
 */
public interface RowSelectionPolicy {

  /**
   * Finds if this <code>genericRecord</code> needs to be audited
   *
   * @param inputRecord to be audited
   * @return <code>true</code> if <code>inputRecord</code> needs to be audited <code>false</code> otherwise
   */
  public boolean shouldSelectRow(GenericRecord inputRecord);

}
