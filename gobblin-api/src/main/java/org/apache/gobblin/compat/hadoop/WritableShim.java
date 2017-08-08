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
package org.apache.gobblin.compat.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * An interface that mirrors Hadoop's Writable interface; this allows objects in gobblin-api
 * to implement similar serializers without explicitly depending on Hadoop itself for the
 * interface definition.
 *
 * Note: For deserialization to work, classes must either implement a no-parameter constructor
 * or always pass an item for re-use in the Hadoop deserialize() call.
 */
public interface WritableShim {
  void readFields(DataInput in) throws IOException;
  void write(DataOutput out) throws IOException;
}
