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

package gobblin.policies.avro;

import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;


/**
 * A class that checks whether an Avro record has header.time or header.timestamp field.
 *
 * @author Ziyang Liu
 */
public class AvroHeaderTimestampPolicy extends RowLevelPolicy {

  public AvroHeaderTimestampPolicy(State state, Type type) {
    super(state, type);
  }

  /**
   * Return PASS if the record has either header.time or header.timestamp field.
   */
  @Override
  public Result executePolicy(Object record) {
    if (!(record instanceof GenericRecord)) {
      return RowLevelPolicy.Result.FAILED;
    }

    GenericRecord header = (GenericRecord) ((GenericRecord) record).get("header");
    if (header == null) {
      return RowLevelPolicy.Result.FAILED;
    }
    if (header.get("time") != null || header.get("timestamp") != null) {
      return RowLevelPolicy.Result.PASSED;
    }
    return RowLevelPolicy.Result.FAILED;
  }
}
