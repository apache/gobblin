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

package gobblin.hive.spec.activity;

import java.io.IOException;
import java.util.List;

import gobblin.hive.HiveRegister;
import gobblin.hive.HiveRegistrationUnit.Column;
import lombok.AllArgsConstructor;


/**
 * An {@link Activity} that drops a collection of Hive partitions given a {@link HiveRegister}.
 *
 * @author Ziyang Liu
 */
@AllArgsConstructor
public class DropPartitionActivity implements Activity {

  protected final String dbName;
  protected final String tableName;
  protected final List<Column> partitionKeys;
  protected final List<String> partitionValues;

  @Override
  public boolean execute(HiveRegister register) throws IOException {
    register.dropPartitionIfExists(this.dbName, this.tableName, this.partitionKeys, this.partitionValues);
    return true;
  }

}
