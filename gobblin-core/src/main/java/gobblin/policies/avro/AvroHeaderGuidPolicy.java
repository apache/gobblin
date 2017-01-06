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
 * A policy that checks whether an Avro record has header.guid field.
 *
 * @author Ziyang Liu
 */
public class AvroHeaderGuidPolicy extends RowLevelPolicy {
  public AvroHeaderGuidPolicy(State state, Type type) {
    super(state, type);
  }

  @Override
  public Result executePolicy(Object record) {
    if (!(record instanceof GenericRecord)) {
      return RowLevelPolicy.Result.FAILED;
    }

    GenericRecord header = (GenericRecord) ((GenericRecord) record).get("header");
    if (header == null || header.get("guid") == null) {
      return RowLevelPolicy.Result.FAILED;
    }
    return RowLevelPolicy.Result.PASSED;
  }
}
