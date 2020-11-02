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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

import org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import org.apache.gobblin.util.FileListUtils;


@Slf4j
public class OrcUtils {
  // For Util class to prevent initialization
  private OrcUtils() {

  }

  public static TypeDescription getTypeDescriptionFromFile(Configuration conf, Path orcFilePath)
      throws IOException {
    return getFileReader(conf, orcFilePath).getSchema();
  }

  /**
   * @deprecated Since the method name isn't accurate. Please calling {@link this#getFileReader(Configuration, Path)}
   * directly
   */
  @Deprecated
  public static Reader getRecordReaderFromFile(Configuration conf, Path orcFilePath) throws IOException {
    return getFileReader(conf, orcFilePath);
  }

  public static Reader getFileReader(Configuration conf, Path orcFilePath)
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
   * This method copies value in object {@param w} into object {@param v} recursively even if the schema of w and v
   * differs in a compatible way, meaning if there's a field existing in v but not in w, the null value will be filled.
   * It served as a helper method for {@link #upConvertOrcStruct(OrcStruct, OrcStruct, TypeDescription)} when OrcStruct
   * contains nested structure as a member.
   *
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
      targetList.clear();

      for (int i = 0; i < castedList.size(); i++) {
        WritableComparable targetListRecordContainer = createValueRecursively(elementType, 0);
        targetList.add(i,
            structConversionHelper((WritableComparable) castedList.get(i), targetListRecordContainer, elementType));
      }
    } else if (w instanceof OrcMap) {
      OrcMap castedMap = (OrcMap) w;
      OrcMap targetMap = (OrcMap) v;
      TypeDescription valueSchema = targetSchema.getChildren().get(1);
      targetMap.clear();

      for (Object entry : castedMap.entrySet()) {
        Map.Entry<WritableComparable, WritableComparable> castedEntry =
            (Map.Entry<WritableComparable, WritableComparable>) entry;
        WritableComparable targetMapRecordContainer = createValueRecursively(valueSchema);
        targetMapRecordContainer =
            structConversionHelper(castedEntry.getValue(), targetMapRecordContainer, valueSchema);
        targetMap.put(castedEntry.getKey(), targetMapRecordContainer);
      }
    } else if (w instanceof OrcUnion) {
      OrcUnion castedUnion = (OrcUnion) w;
      OrcUnion targetUnion = (OrcUnion) v;
      byte tag = castedUnion.getTag();

      // ORC doesn't support Union type widening
      // Avro doesn't allow it either, reference: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
      // As a result, member schema within source and target should be identical.
      TypeDescription targetMemberSchema = targetSchema.getChildren().get(tag);
      targetUnion.set(tag, structConversionHelper((WritableComparable) castedUnion.getObject(),
          (WritableComparable) OrcUtils.createValueRecursively(targetMemberSchema), targetMemberSchema));
    } else {
      // Regardless whether type-widening is happening or not, this method copy the value of w into v.
      handlePrimitiveWritableComparable(w, v);
    }

    // If non-primitive or type-widening is required, v should already be populated by w's value recursively.
    return v;
  }

  /**
   * Recursively convert the {@param oldStruct} into {@param newStruct} whose schema is {@param targetSchema}.
   * This serves similar purpose like GenericDatumReader for Avro, which accepts an reader schema and writer schema
   * to allow users convert bytes into reader's schema in a compatible approach.
   * Calling this method SHALL NOT cause any side-effect for {@param oldStruct}, also it will copy value of each fields
   * in {@param oldStruct} into {@param newStruct} recursively. Please ensure avoiding unnecessary call as it could
   * be pretty expensive if the struct schema is complicated, or contains container objects like array/map.
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

    int indexInNewSchema = 0;
    List<String> oldSchemaFieldNames = oldStruct.getSchema().getFieldNames();
    /* Construct a fieldName -> Index map to efficient access within the loop below. */
    Map<String, Integer> oldSchemaIndex = IntStream.range(0, oldSchemaFieldNames.size()).boxed()
        .collect(Collectors.toMap(oldSchemaFieldNames::get, Function.identity()));
    List<TypeDescription> oldSchemaTypes = oldStruct.getSchema().getChildren();
    List<TypeDescription> newSchemaTypes = targetSchema.getChildren();

    for (String fieldName : targetSchema.getFieldNames()) {
      if (oldSchemaFieldNames.contains(fieldName) && oldStruct.getFieldValue(fieldName) != null) {
        int fieldIndex = oldSchemaIndex.get(fieldName);

        TypeDescription oldFieldSchema = oldSchemaTypes.get(fieldIndex);
        TypeDescription newFieldSchema = newSchemaTypes.get(indexInNewSchema);

        if (isEvolutionValid(oldFieldSchema, newFieldSchema)) {
          WritableComparable oldField = oldStruct.getFieldValue(fieldName);
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
   * Copy the value of {@param from} object into {@param to} with supporting of type-widening that ORC allowed.
   */
  public static void handlePrimitiveWritableComparable(WritableComparable from, WritableComparable to) {
    if (from instanceof ByteWritable) {
      if (to instanceof ByteWritable) {
        ((ByteWritable) to).set(((ByteWritable) from).get());
        return;
      } else if (to instanceof ShortWritable) {
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
      if (to instanceof ShortWritable) {
        ((ShortWritable) to).set(((ShortWritable) from).get());
        return;
      } else if (to instanceof IntWritable) {
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
      if (to instanceof IntWritable) {
        ((IntWritable) to).set(((IntWritable) from).get());
        return;
      } else if (to instanceof LongWritable) {
        ((LongWritable) to).set(((IntWritable) from).get());
        return;
      } else if (to instanceof DoubleWritable) {
        ((DoubleWritable) to).set(((IntWritable) from).get());
        return;
      }
    } else if (from instanceof LongWritable) {
      if (to instanceof LongWritable) {
        ((LongWritable) to).set(((LongWritable) from).get());
        return;
      } else if (to instanceof DoubleWritable) {
        ((DoubleWritable) to).set(((LongWritable) from).get());
        return;
      }
      // Following from this branch, type-widening is not allowed and only value-copy will happen.
    } else if (from instanceof DoubleWritable) {
      if (to instanceof DoubleWritable) {
        ((DoubleWritable) to).set(((DoubleWritable) from).get());
        return;
      }
    } else if (from instanceof BytesWritable) {
      if (to instanceof BytesWritable) {
        ((BytesWritable) to).set((BytesWritable) from);
        return;
      }
    } else if (from instanceof FloatWritable) {
      if (to instanceof FloatWritable) {
        ((FloatWritable) to).set(((FloatWritable) from).get());
        return;
      }
    } else if (from instanceof Text) {
      if (to instanceof Text) {
        ((Text) to).set((Text) from);
        return;
      }
    } else if (from instanceof DateWritable) {
      if (to instanceof DateWritable) {
        ((DateWritable) to).set(((DateWritable) from).get());
        return;
      }
    } else if (from instanceof OrcTimestamp && to instanceof OrcTimestamp) {
      ((OrcTimestamp) to).set(((OrcTimestamp) from).toString());
      return;
    } else if (from instanceof HiveDecimalWritable && to instanceof HiveDecimalWritable) {
      ((HiveDecimalWritable) to).set(((HiveDecimalWritable) from).getHiveDecimal());
      return;
    } else if (from instanceof BooleanWritable && to instanceof BooleanWritable) {
      ((BooleanWritable) to).set(((BooleanWritable) from).get());
      return;
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
        // For union, there's no way to determine which tag's object type to create with only schema.
        // It can be determined in the cases when a OrcUnion's value needs to be copied to another object recursively,
        // and the source OrcUnion can provide this information.
        return new OrcUnion(schema);
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

  /**
   * Check recursively if owning schema is eligible to be up-converted to targetSchema if
   * TargetSchema is a subset of originalSchema
   */
  public static boolean eligibleForUpConvertHelper(TypeDescription originalSchema, TypeDescription targetSchema) {
    if (!targetSchema.getCategory().isPrimitive()) {
      if (originalSchema.getCategory() != targetSchema.getCategory()) {
        return false;
      }

      if (targetSchema.getCategory().equals(TypeDescription.Category.LIST)) {
        Preconditions
            .checkArgument(originalSchema.getChildren() != null, "Illegal format of ORC schema as:" + originalSchema);
        return eligibleForUpConvertHelper(originalSchema.getChildren().get(0), targetSchema.getChildren().get(0));
      } else if (targetSchema.getCategory().equals(TypeDescription.Category.MAP)) {
        Preconditions
            .checkArgument(originalSchema.getChildren() != null, "Illegal format of ORC schema as:" + originalSchema);

        return eligibleForUpConvertHelper(originalSchema.getChildren().get(0), targetSchema.getChildren().get(0))
            && eligibleForUpConvertHelper(originalSchema.getChildren().get(1), targetSchema.getChildren().get(1));
      } else if (targetSchema.getCategory().equals(TypeDescription.Category.UNION)) {
        // we don't project into union as shuffle key.
        return true;
      } else if (targetSchema.getCategory().equals(TypeDescription.Category.STRUCT)) {
        if (!originalSchema.getFieldNames().containsAll(targetSchema.getFieldNames())) {
          return false;
        }

        boolean result = true;

        for (int i = 0; i < targetSchema.getFieldNames().size(); i++) {
          String subSchemaFieldName = targetSchema.getFieldNames().get(i);
          result &= eligibleForUpConvertHelper(originalSchema.findSubtype(subSchemaFieldName),
              targetSchema.getChildren().get(i));
        }

        return result;
      } else {
        // There are totally 5 types of non-primitive. If falling into this branch, it means it is a TIMESTAMP_INSTANT
        // and we will by default treated it as eligible.
        return true;
      }
    } else {
      // Check the unit type: Only for the category.
      return originalSchema.getCategory().equals(targetSchema.getCategory());
    }
  }

  // Eligibility for up-conversion: If targetSchema is a subset of originalSchema (Schema projection)
  // and vice-versa (schema expansion).
  public static boolean eligibleForUpConvert(TypeDescription originalSchema, TypeDescription targetSchema) {
    return eligibleForUpConvertHelper(originalSchema, targetSchema) || eligibleForUpConvertHelper(targetSchema,
        originalSchema);
  }
}
