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

package gobblin.compaction.mapreduce.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import com.google.common.base.Splitter;


/**
 * Job config based {@link AvroDeltaFieldNameProvider}, which reads delta fields from config properties.
 */
@Test(groups = {"gobblin.compaction"})
public class ConfBasedDeltaFieldProvider implements AvroDeltaFieldNameProvider {
  public static final String DELTA_FIELDS_KEY =
      "gobblin.compaction." + ConfBasedDeltaFieldProvider.class.getSimpleName() + ".deltaFields";
  private final List<String> deltaFields;

  public ConfBasedDeltaFieldProvider(Configuration conf) {
    String deltaConfValue = conf.get(DELTA_FIELDS_KEY);
    if (deltaConfValue == null) {
      this.deltaFields = new ArrayList<>();
    } else {
      this.deltaFields = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(deltaConfValue);
    }
  }

  /**
   * Return delta fields specified by {@link #DELTA_FIELDS_KEY}.
   * The order of the returned list is consistent with the order in job conf.
   */
  public List<String> getDeltaFieldNames(GenericRecord record) {
    return this.deltaFields;
  }
}
