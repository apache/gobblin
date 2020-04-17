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
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import org.apache.gobblin.util.FileListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.orc.mapred.OrcUnion;
import org.apache.orc.mapred.OrcValue;


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
   * Implementation stolen and manipulated from {@link SchemaEvolution} as that was package-private.
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
   * - We only give 3 primitive values and use them to construct compound values.
   * - For List, Map or Union, make sure there's at least one entry within the record-container.
   */
  public static void orcStructFillerWithFixedValue(WritableComparable w, TypeDescription schema, int intValue,
      String string, boolean booleanValue) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        ((BooleanWritable) w).set(booleanValue);
        break;
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        ((IntWritable) w).set(intValue);
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
          orcStructFillerWithFixedValue((WritableComparable) i, schema.getChildren().get(0), intValue, string, booleanValue);
        }
        break;
      case MAP:
        OrcMap castedMap = (OrcMap) w;
        for (Object entry : castedMap.entrySet()) {
          Map.Entry<WritableComparable, WritableComparable> castedEntry =
              (Map.Entry<WritableComparable, WritableComparable>) entry;
          orcStructFillerWithFixedValue(castedEntry.getKey(), schema.getChildren().get(0), intValue, string, booleanValue);
          orcStructFillerWithFixedValue(castedEntry.getValue(), schema.getChildren().get(1), intValue, string, booleanValue);
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
        byte tag = castedUnion.getTag();
        orcStructFillerWithFixedValue((WritableComparable) castedUnion.getObject(), schema.getChildren().get(tag),
            intValue, string, booleanValue);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + schema.toString());
    }
  }

  /**
   * Given a schema, fill some random value in each of field in the given record container {@param w},
   * assuming {@param w} has exactly the same schema as the given schema.
   *
   * Only support limited value-type since this is mostly used for testing purpose when generating datasets given
   * a schema is necessary since pulling original dataset could run into compliance concern.
   */
  @SuppressWarnings("unchecked")
  public static void randomFillOrcStructWithAnySchema(WritableComparable w, TypeDescription schema, int seed) {

    //Sort of give randomness for current method call.
    int randomNum = new Random(seed).nextInt();
    byte[] array = new byte[4]; // length is bounded by 4
    new Random().nextBytes(array);
    String generatedString = new String(array, StandardCharsets.UTF_8);

    orcStructFillerWithFixedValue(w, schema, randomNum, generatedString, randomNum % 2 == 0);
  }
}
