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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.compaction.mapreduce.RecordKeyMapperBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;


/**
 * To keep consistent with {@link OrcMapreduceRecordReader}'s decision on implementing
 * {@link RecordReader} with {@link NullWritable} as the key and generic type of value, the ORC Mapper will
 * read in the record as the input value.
 */
@Slf4j
public class OrcValueMapper extends RecordKeyMapperBase<NullWritable, OrcStruct, Object, OrcValue> {

  // This key will only be initialized lazily when dedup is enabled.
  private OrcKey outKey;
  private OrcValue outValue;
  private TypeDescription mrOutputSchema;
  private JobConf jobConf;

  // This is added mostly for debuggability.
  private static int writeCount = 0;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    this.jobConf = new JobConf(context.getConfiguration());
    this.outKey.configure(jobConf);
    this.outValue = new OrcValue();
    this.mrOutputSchema =
        TypeDescription.fromString(context.getConfiguration().get(OrcConf.MAPRED_INPUT_SCHEMA.getAttribute()));
  }

  @Override
  protected void map(NullWritable key, OrcStruct orcStruct, Context context)
      throws IOException, InterruptedException {
    upConvertOrcStruct(orcStruct, (OrcStruct) outValue.value, mrOutputSchema);
    try {
      if (context.getNumReduceTasks() == 0) {
        context.write(NullWritable.get(), this.outValue);
      } else {
//        fillDedupKey(upConvertedOrcStruct);
        context.write(this.outKey, this.outValue);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failure in write record no." + writeCount, e);
    }
    writeCount += 1;

    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);
  }

  /**
   * Recursively up-convert the {@link OrcStruct} into newStruct's schema which is {@link #mrOutputSchema}.
   * Limitation:
   * 1. Does not support up-conversion of key types in Maps
   * 2. Conversion only happens if org.apache.gobblin.compaction.mapreduce.orc.OrcValueMapper#isEvolutionValid return true.
   */
  @VisibleForTesting
  void upConvertOrcStruct(OrcStruct oldStruct, OrcStruct newStruct, TypeDescription targetSchema) {

    // If target schema is not equal to newStruct's schema, it is a illegal state and doesn't make sense to work through.
    Preconditions.checkArgument(newStruct.getSchema().equals(targetSchema));

    // For ORC schema, if schema object differs that means schema itself is different while for Avro,
    // there are chances that documentation or attributes' difference lead to the schema object difference.
    if (!oldStruct.getSchema().equals(targetSchema)) {
      log.info("There's schema mismatch identified from reader's schema and writer's schema");

      int indexInNewSchema = 0;
      List<String> oldSchemaFieldNames = oldStruct.getSchema().getFieldNames();
      List<TypeDescription> oldSchemaTypes = oldStruct.getSchema().getChildren();
      List<TypeDescription> newSchemaTypes = targetSchema.getChildren();

      for (String fieldName : targetSchema.getFieldNames()) {
        if (oldSchemaFieldNames.contains(fieldName)) {
          int fieldIndex = oldSchemaFieldNames.indexOf(fieldName);

          TypeDescription fileType = oldSchemaTypes.get(fieldIndex);
          TypeDescription readerType = newSchemaTypes.get(indexInNewSchema);

          if (OrcUtils.isEvolutionValid(fileType, readerType)) {
            WritableComparable oldField = oldStruct.getFieldValue(fieldName);
            WritableComparable newField = newStruct.getFieldValue(fieldName);
            structConversionHelper(oldField, newField, targetSchema.getChildren().get(fieldIndex));
            newStruct.setFieldValue(fieldName, oldField);
          } else {
            throw new SchemaEvolution.IllegalEvolutionException(String
                .format("ORC does not support type conversion from file" + " type %s to reader type %s ",
                    fileType.toString(), readerType.toString()));
          }
        } else {
          newStruct.setFieldValue(fieldName, null);
        }

        indexInNewSchema++;
      }

    }
  }

  /**
   * Suppress the warning of type checking: All casts are clearly valid as they are all (sub)elements Orc types.
   * Check failure will trigger Cast exception and blow up the process.
   */
  @SuppressWarnings("unchecked")
  private void structConversionHelper(WritableComparable w, WritableComparable v, TypeDescription mapperSchema) {
    if (w instanceof OrcStruct) {
      upConvertOrcStruct((OrcStruct) w, (OrcStruct) v, mapperSchema);
    } else if (w instanceof OrcList) {
      OrcList castedList = (OrcList) w;
      OrcList targetList = (OrcList) v;
      TypeDescription elementType = mapperSchema.getChildren().get(0);
      for (int i = 0; i < castedList.size(); i++) {
        structConversionHelper((WritableComparable) castedList.get(i), (WritableComparable) targetList.get(i), elementType);
      }
    } else if (w instanceof OrcMap) {
      OrcMap castedMap = (OrcMap) w;
      OrcMap targetMap = (OrcMap) v;
      for (Object entry : castedMap.entrySet()) {
        Map.Entry<WritableComparable, WritableComparable> castedEntry =
            (Map.Entry<WritableComparable, WritableComparable>) entry;
        structConversionHelper(castedEntry.getValue(),(WritableComparable) targetMap.get(((Map.Entry<WritableComparable, WritableComparable>) entry).getKey()) , mapperSchema.getChildren().get(1));
      }
    } else if (w instanceof OrcUnion) {
      OrcUnion castedUnion = (OrcUnion) w;
      OrcUnion targetUnion = (OrcUnion) v;
      byte tag = castedUnion.getTag();
      structConversionHelper((WritableComparable) castedUnion.getObject(),
          (WritableComparable) targetUnion.getObject(), mapperSchema.getChildren().get(tag));
    }

    // Do nothing if primitive object.
  }

  /**
   * By default, dedup key contains the whole ORC record, except MAP since {@link org.apache.orc.mapred.OrcMap} is
   * an implementation of {@link java.util.TreeMap} which doesn't accept difference of records within the map in comparison.
   */
  protected void fillDedupKey(OrcStruct originalRecord) {
    // TODO: Should leverage upconvert method.
  }
}
