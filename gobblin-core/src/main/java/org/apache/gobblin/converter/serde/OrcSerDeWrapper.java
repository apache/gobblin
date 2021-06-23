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

package gobblin.converter.serde;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.Writable;

/**
 * The Hive's {@link OrcSerde} caches converted records - the {@link OrcSerde} has a single
 * {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow} and every time the
 * {@link org.apache.hadoop.hive.serde2.Serializer#serialize(Object, ObjectInspector)} method is called, the object is
 * re-used.
 *
 * The problem is that {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow} is package protected and has no
 * public constructor, so no copy can be made. This would be fine if {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow}
 * is immediately written out. But all Gobblin jobs have a buffer that the writer reads from. This buffering can cause
 * race conditions where records get dropped and duplicated.
 *
 * @author Prateek Gupta
 */

public class OrcSerDeWrapper extends OrcSerde {

  @Override
  public Writable serialize(Object realRow, ObjectInspector inspector) {
      Object realRowClone = ObjectInspectorUtils.copyToStandardObject(realRow, inspector);
      return super.serialize(realRowClone, inspector);
  }
}
