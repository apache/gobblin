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

package org.apache.gobblin.compaction.mapreduce.orc;

import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;


/**
 * Compare {@link OrcKey} in shuffle of MapReduce.
 * Delegate byte decoding to underlying {@link OrcStruct#readFields(DataInput)} method to simplify comparison.
 */
public class OrcKeyComparator extends Configured implements RawComparator<OrcKey> {
  private TypeDescription schema;
  private OrcKey key1;
  private OrcKey key2;
  private DataInputBuffer buffer;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (null != conf) {
      // The MapReduce framework will be using this comparator to sort OrcKey objects
      // output from the map phase, so use the schema defined for the map output key
      // and the data model non-raw compare() implementation.
      schema = TypeDescription.fromString(conf.get(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute()));
      OrcStruct orcRecordModel1 = (OrcStruct) OrcStruct.createValue(schema);
      OrcStruct orcRecordModel2 = (OrcStruct) OrcStruct.createValue(schema);

      if (key1 == null) {
        key1 = new OrcKey();
      }
      if (key2 == null) {
        key2 = new OrcKey();
      }
      if (buffer == null) {
        buffer = new DataInputBuffer();
      }

      key1.key = orcRecordModel1;
      key2.key = orcRecordModel2;
    }
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    try {
      buffer.reset(b1, s1, l1);      // parse key1
      key1.readFields(buffer);

      buffer.reset(b2, s2, l2);      // parse key2
      key2.readFields(buffer);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return compare(key1, key2);     // compare them
  }

  @Override
  public int compare(OrcKey o1, OrcKey o2) {
    if (!(o1.key instanceof OrcStruct) || !(o2.key instanceof OrcStruct)) {
      throw new IllegalStateException("OrcKey should have its key value be instance of OrcStruct");
    }
    return ((OrcStruct) o1.key).compareTo((OrcStruct) o2.key);
  }
}
