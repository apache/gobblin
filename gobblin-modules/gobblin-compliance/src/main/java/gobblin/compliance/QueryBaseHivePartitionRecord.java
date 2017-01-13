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
package gobblin.compliance;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Partition;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;


/**
 * Record corresponding to a hive partition. It also contains a list of queries to be executed on this partition.
 */
@Getter
@Setter
public abstract class QueryBaseHivePartitionRecord {
  protected Partition hivePartition;
  protected List<String> queries;
}
