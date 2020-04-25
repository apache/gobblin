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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import org.apache.gobblin.util.FileListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ConvertTreeReaderFactory;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.orc.mapred.OrcUnion;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class OrcUtils {
  // For Util class to prevent initialization
  private OrcUtils() {

  }

  public static TypeDescription getTypeDescriptionFromFile(Configuration conf, Path orcFilePath)
      throws IOException {
    return getRecordReaderFromFile(conf, orcFilePath).getSchema();
  }

  public static Reader getRecordReaderFromFile(Configuration conf, Path orcFilePath)
      throws IOException {
    return OrcFile.createReader(orcFilePath, new OrcFile.ReaderOptions(conf));
  }

  public static TypeDescription getNewestSchemaFromSource(Job job, FileSystem fs)
      throws IOException {
    Path[] sourceDirs = FileInputFormat.getInputPaths(job);
    if (sourceDirs.length == 0) {
      throw new IllegalStateException("There should be at least one directory specified for the MR job");
    }

    List<FileStatus> files = new ArrayList<FileStatus>();

    for (Path sourceDir : sourceDirs) {
      files.addAll(FileListUtils.listFilesRecursively(fs, sourceDir));
    }
    Collections.sort(files, new MRCompactorAvroKeyDedupJobRunner.LastModifiedDescComparator());

    TypeDescription resultSchema;
    for (FileStatus status : files) {
      resultSchema = getTypeDescriptionFromFile(job.getConfiguration(), status.getPath());
      if (resultSchema != null) {
        return resultSchema;
      }
    }

    throw new IllegalStateException(String
        .format("There's no file carrying orc file schema in the list of directories: %s",
            Arrays.toString(sourceDirs)));
  }

  /**
   * Determine if two types are following valid evolution.
   * Implementation taken and manipulated from {@link SchemaEvolution} as that was package-private.
   */
  static boolean isEvolutionValid(TypeDescription fileType, TypeDescription readerType) {
    boolean isOk = true;
    if (fileType.getCategory() == readerType.getCategory()) {
      switch (readerType.getCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DOUBLE:
        case FLOAT:
        case STRING:
        case TIMESTAMP:
        case BINARY:
        case DATE:
          // these are always a match
          break;
        case CHAR:
        case VARCHAR:
          break;
        case DECIMAL:
          break;
        case UNION:
        case MAP:
        case LIST: {
          // these must be an exact match
          List<TypeDescription> fileChildren = fileType.getChildren();
          List<TypeDescription> readerChildren = readerType.getChildren();
          if (fileChildren.size() == readerChildren.size()) {
            for (int i = 0; i < fileChildren.size(); ++i) {
              isOk &= isEvolutionValid(fileChildren.get(i), readerChildren.get(i));
            }
            return isOk;
          } else {
            return false;
          }
        }
        case STRUCT: {
          List<TypeDescription> readerChildren = readerType.getChildren();
          List<TypeDescription> fileChildren = fileType.getChildren();

          List<String> readerFieldNames = readerType.getFieldNames();
          List<String> fileFieldNames = fileType.getFieldNames();

          final Map<String, TypeDescription> fileTypesIdx = new HashMap();
          for (int i = 0; i < fileFieldNames.size(); i++) {
            final String fileFieldName = fileFieldNames.get(i);
            fileTypesIdx.put(fileFieldName, fileChildren.get(i));
          }

          for (int i = 0; i < readerFieldNames.size(); i++) {
            final String readerFieldName = readerFieldNames.get(i);
            TypeDescription readerField = readerChildren.get(i);
            TypeDescription fileField = fileTypesIdx.get(readerFieldName);
            if (fileField == null) {
              continue;
            }

            isOk &= isEvolutionValid(fileField, readerField);
          }
          return isOk;
        }
        default:
          throw new IllegalArgumentException("Unknown type " + readerType);
      }
      return isOk;
    } else {
      /*
       * Check for the few cases where will not convert....
       */
      return ConvertTreeReaderFactory.canConvert(fileType, readerType);
    }
  }

  /**
   * Fill in value in OrcStruct with given schema, assuming {@param w} contains the same schema as {@param schema}.
   * {@param schema} is still necessary to given given {@param w} do contains schema information itself, because the
   * actual value type is only available in {@link TypeDescription} but not {@link org.apache.orc.mapred.OrcValue}.
   *
   * For simplicity here are some assumptions:
   * - We only give 3 primitive values and use them to construct compound values. To make it work for different types that
   * can be widened or shrunk to each other, please use value within small range.
   * - For List, Map or Union, make sure there's at least one entry within the record-container.
   * you may want to try {@link #createValueRecursively(TypeDescription)} instead of {@link OrcStruct#createValue(TypeDescription)}
   */
  public static void orcStructFillerWithFixedValue(WritableComparable w, TypeDescription schema, int intValue,
      String string, boolean booleanValue) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        ((BooleanWritable) w).set(booleanValue);
        break;
      case BYTE:
        ((ByteWritable) w).set((byte) intValue);
        break;
      case SHORT:
        ((ShortWritable) w).set((short) intValue);
        break;
      case INT:
        ((IntWritable) w).set(intValue);
        break;
      case LONG:
        ((LongWritable) w).set(intValue);
        break;
      case FLOAT:
        ((FloatWritable) w).set(intValue * 1.0f);
        break;
      case DOUBLE:
        ((DoubleWritable) w).set(intValue * 1.0);
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        ((Text) w).set(string);
        break;
      case BINARY:
        throw new UnsupportedOperationException("Binary type is not supported in random orc data filler");
      case DECIMAL:
        throw new UnsupportedOperationException("Decimal type is not supported in random orc data filler");
      case DATE:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        throw new UnsupportedOperationException(
            "Timestamp and its derived types is not supported in random orc data filler");
      case LIST:
        OrcList castedList = (OrcList) w;
        // Here it is not trivial to create typed-object in element-type. So this method expect the value container
        // to at least contain one element, or the traversing within the list will be skipped.
        for (Object i : castedList) {
          orcStructFillerWithFixedValue((WritableComparable) i, schema.getChildren().get(0), intValue, string,
              booleanValue);
        }
        break;
      case MAP:
        OrcMap castedMap = (OrcMap) w;
        for (Object entry : castedMap.entrySet()) {
          Map.Entry<WritableComparable, WritableComparable> castedEntry =
              (Map.Entry<WritableComparable, WritableComparable>) entry;
          orcStructFillerWithFixedValue(castedEntry.getKey(), schema.getChildren().get(0), intValue, string,
              booleanValue);
          orcStructFillerWithFixedValue(castedEntry.getValue(), schema.getChildren().get(1), intValue, string,
              booleanValue);
        }
        break;
      case STRUCT:
        OrcStruct castedStruct = (OrcStruct) w;
        int fieldIdx = 0;
        for (TypeDescription child : schema.getChildren()) {
          orcStructFillerWithFixedValue(castedStruct.getFieldValue(fieldIdx), child, intValue, string, booleanValue);
          fieldIdx += 1;
        }
        break;
      case UNION:
        OrcUnion castedUnion = (OrcUnion) w;
        byte tag = 0;
        orcStructFillerWithFixedValue((WritableComparable) castedUnion.getObject(), schema.getChildren().get(tag),
            intValue, string, booleanValue);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + schema.toString());
    }
  }

  /**
   * Suppress the warning of type checking: All casts are clearly valid as they are all (sub)elements Orc types.
   * Check failure will trigger Cast exception and blow up the process.
   */
  @SuppressWarnings("unchecked")
  private static WritableComparable structConversionHelper(WritableComparable w, WritableComparable v,
      TypeDescription targetSchema) {

    if (w instanceof OrcStruct) {
      upConvertOrcStruct((OrcStruct) w, (OrcStruct) v, targetSchema);
    } else if (w instanceof OrcList) {
      OrcList castedList = (OrcList) w;
      OrcList targetList = (OrcList) v;
      TypeDescription elementType = targetSchema.getChildren().get(0);
      WritableComparable targetListRecordContainer =
          targetList.size() > 0 ? (WritableComparable) targetList.get(0) : createValueRecursively(elementType, 0);
      targetList.clear();

      for (int i = 0; i < castedList.size(); i++) {
        targetList.add(i,
            structConversionHelper((WritableComparable) castedList.get(i), targetListRecordContainer, elementType));
      }
    } else if (w instanceof OrcMap) {
      OrcMap castedMap = (OrcMap) w;
      OrcMap targetMap = (OrcMap) v;
      TypeDescription valueSchema = targetSchema.getChildren().get(1);

      // Create recordContainer with the schema of value.
      Iterator targetMapEntries = targetMap.values().iterator();
      WritableComparable targetMapRecordContainer =
          targetMapEntries.hasNext() ? (WritableComparable) targetMapEntries.next()
              : createValueRecursively(valueSchema);

      targetMap.clear();

      for (Object entry : castedMap.entrySet()) {
        Map.Entry<WritableComparable, WritableComparable> castedEntry =
            (Map.Entry<WritableComparable, WritableComparable>) entry;

        targetMapRecordContainer =
            structConversionHelper(castedEntry.getValue(), targetMapRecordContainer, valueSchema);
        targetMap.put(castedEntry.getKey(), targetMapRecordContainer);
      }
    } else if (w instanceof OrcUnion) {
      OrcUnion castedUnion = (OrcUnion) w;
      OrcUnion targetUnion = (OrcUnion) v;
      byte tag = castedUnion.getTag();

      targetUnion.set(tag, structConversionHelper((WritableComparable) castedUnion.getObject(),
          (WritableComparable) targetUnion.getObject(), targetSchema.getChildren().get(tag)));
    } else {
      // If primitive without type-widening, return the oldField's value which is inside w to avoid value-deepCopy from w to v.
      if (w.getClass().equals(v.getClass())) {
        return w;
      } else {
        // If type widening is required, this method copy the value of w into v.
        writableComparableTypeWidening(w, v);
      }
    }

    // If non-primitive or type-widening is required, v should already be populated by w's value recursively.
    return v;
  }

  /**
   * Recursively convert the {@param oldStruct} into {@param newStruct} whose schema is {@param targetSchema}.
   * This serves similar purpose like GenericDatumReader for Avro, which accepts an reader schema and writer schema
   * to allow users convert bytes into reader's schema in a compatible approach.
   * Calling this method SHALL NOT cause any side-effect for {@param oldStruct}, also it will copy value of each fields
   * in {@param oldStruct} into {@param newStruct} recursively. Please ensure avoiding unnecessary call.
   *
   * Note that if newStruct containing things like List/Map (container-type), the up-conversion is doing two things:
   * 1. Clear all elements in original containers.
   * 2. Make value of container elements in {@param oldStruct} is populated into {@param newStruct} with element-type
   * in {@param newStruct} if compatible.
   *
   * Limitation:
   * 1. Does not support up-conversion of key types in Maps. The underlying reasoning is because of the primary format
   * from upstream is Avro, which enforces key-type to be string only.
   * 2. Conversion from a field A to field B only happens if
   * org.apache.gobblin.compaction.mapreduce.orc.OrcValueMapper#isEvolutionValid(A,B) return true.
   */
  @VisibleForTesting
  public static void upConvertOrcStruct(OrcStruct oldStruct, OrcStruct newStruct, TypeDescription targetSchema) {

    // If target schema is not equal to newStruct's schema, it is a illegal state and doesn't make sense to work through.
    Preconditions.checkArgument(newStruct.getSchema().equals(targetSchema));
    log.info("There's schema mismatch identified from reader's schema and writer's schema");

    int indexInNewSchema = 0;
    List<String> oldSchemaFieldNames = oldStruct.getSchema().getFieldNames();
    List<TypeDescription> oldSchemaTypes = oldStruct.getSchema().getChildren();
    List<TypeDescription> newSchemaTypes = targetSchema.getChildren();

    for (String fieldName : targetSchema.getFieldNames()) {
      if (oldSchemaFieldNames.contains(fieldName)) {
        int fieldIndex = oldSchemaFieldNames.indexOf(fieldName);

        TypeDescription oldFieldSchema = oldSchemaTypes.get(fieldIndex);
        TypeDescription newFieldSchema = newSchemaTypes.get(indexInNewSchema);

        if (isEvolutionValid(oldFieldSchema, newFieldSchema)) {
          WritableComparable oldField = oldStruct.getFieldValue(fieldName);
          oldField = (oldField == null) ? OrcUtils.createValueRecursively(oldFieldSchema) : oldField;
          WritableComparable newField = newStruct.getFieldValue(fieldName);
          newField = (newField == null) ? OrcUtils.createValueRecursively(newFieldSchema) : newField;
          newStruct.setFieldValue(fieldName, structConversionHelper(oldField, newField, newFieldSchema));
        } else {
          throw new SchemaEvolution.IllegalEvolutionException(String
              .format("ORC does not support type conversion from file" + " type %s to reader type %s ",
                  oldFieldSchema.toString(), newFieldSchema.toString()));
        }
      } else {
        newStruct.setFieldValue(fieldName, null);
      }

      indexInNewSchema++;
    }
  }

  /**
   * For primitive types of {@link WritableComparable}, supporting ORC-allowed type-widening.
   */
  public static void writableComparableTypeWidening(WritableComparable from, WritableComparable to) {
    if (from instanceof ByteWritable) {
      if (to instanceof ShortWritable) {
        ((ShortWritable) to).set(((ByteWritable) from).get());
        return;
      } else if (to instanceof IntWritable) {
        ((IntWritable) to).set(((ByteWritable) from).get());
        return;
      } else if (to instanceof LongWritable) {
        ((LongWritable) to).set(((ByteWritable) from).get());
        return;
      } else if (to instanceof DoubleWritable) {
        ((DoubleWritable) to).set(((ByteWritable) from).get());
        return;
      }
    } else if (from instanceof ShortWritable) {
      if (to instanceof IntWritable) {
        ((IntWritable) to).set(((ShortWritable) from).get());
        return;
      } else if (to instanceof LongWritable) {
        ((LongWritable) to).set(((ShortWritable) from).get());
        return;
      } else if (to instanceof DoubleWritable) {
        ((DoubleWritable) to).set(((ShortWritable) from).get());
        return;
      }
    } else if (from instanceof IntWritable) {
      if (to instanceof LongWritable) {
        ((LongWritable) to).set(((IntWritable) from).get());
        return;
      } else if (to instanceof DoubleWritable) {
        ((DoubleWritable) to).set(((IntWritable) from).get());
        return;
      }
    } else if (from instanceof LongWritable) {
      if (to instanceof DoubleWritable) {
        ((DoubleWritable) to).set(((LongWritable) from).get());
        return;
      }
    }
    throw new UnsupportedOperationException(String
        .format("The conversion of primitive-type WritableComparable object from %s to %s is not supported",
            from.getClass(), to.getClass()));
  }

  /**
   * For nested structure like struct<a:array<struct<int,string>>>, calling OrcStruct.createValue doesn't create entry for the inner
   * list, which would be required to assign a value if the entry-type has nested structure, or it just cannot see the
   * entry's nested structure.
   *
   * This function should be fed back to open-source ORC.
   */
  public static WritableComparable createValueRecursively(TypeDescription schema, int elemNum) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanWritable();
      case BYTE:
        return new ByteWritable();
      case SHORT:
        return new ShortWritable();
      case INT:
        return new IntWritable();
      case LONG:
        return new LongWritable();
      case FLOAT:
        return new FloatWritable();
      case DOUBLE:
        return new DoubleWritable();
      case BINARY:
        return new BytesWritable();
      case CHAR:
      case VARCHAR:
      case STRING:
        return new Text();
      case DATE:
        return new DateWritable();
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new OrcTimestamp();
      case DECIMAL:
        return new HiveDecimalWritable();
      case STRUCT: {
        OrcStruct result = new OrcStruct(schema);
        int c = 0;
        for (TypeDescription child : schema.getChildren()) {
          result.setFieldValue(c++, createValueRecursively(child, elemNum));
        }
        return result;
      }
      case UNION: {
        OrcUnion result = new OrcUnion(schema);
        result.set(0, createValueRecursively(schema.getChildren().get(0), elemNum));
        return result;
      }
      case LIST: {
        OrcList result = new OrcList(schema);
        for (int i = 0; i < elemNum; i++) {
          result.add(createValueRecursively(schema.getChildren().get(0), elemNum));
        }
        return result;
      }
      case MAP: {
        OrcMap result = new OrcMap(schema);
        for (int i = 0; i < elemNum; i++) {
          result.put(createValueRecursively(schema.getChildren().get(0), elemNum),
              createValueRecursively(schema.getChildren().get(1), elemNum));
        }
        return result;
      }
      default:
        throw new IllegalArgumentException("Unknown type " + schema);
    }
  }

  public static WritableComparable createValueRecursively(TypeDescription schema) {
    return createValueRecursively(schema, 1);
  }
}
