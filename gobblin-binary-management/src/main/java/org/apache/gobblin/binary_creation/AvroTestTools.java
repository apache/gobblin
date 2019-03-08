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

package org.apache.gobblin.binary_creation;

import com.google.common.collect.AbstractIterator;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.FsInput;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filters.HiddenFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ConfigurationBuilder;


/**
 * A implementation of {@link DataTestTools} for Avro format.
 */
@Slf4j
public class AvroTestTools extends DataTestTools<AvroTestTools.RecordIterator, Schema> {

  public boolean checkSameFilesAndRecords(TreeMap<String, RecordIterator> expected,
      TreeMap<String, RecordIterator> observed, boolean allowDifferentOrder, Collection<String> blacklistRecordFields,
      boolean allowDifferentSchema) {
    Iterator<String> keys1 = expected.navigableKeySet().iterator();
    Iterator<String> keys2 = observed.navigableKeySet().iterator();

    return compareIterators(keys1, keys2, (key1, key2) -> {
      if (!removeExtension(key1).equals(removeExtension(key2))) {
        log.error(String.format("Mismatched files: %s and %s", key1, key2));
        return false;
      }

      RecordIterator it1 = expected.get(key1);
      RecordIterator it2 = observed.get(key2);

      if (!allowDifferentSchema && !it1.getSchema().equals(it2.getSchema())) {
        log.error(String.format("Mismatched schemas: %s and %s", key1, key2));
        return false;
      }

      if (allowDifferentOrder) {
        Set r1 = allowDifferentSchema
            ? toSetWithBlacklistedFields(it1, blacklistRecordFields, GenericRecordWrapper::new)
            : toSetWithBlacklistedFields(it1, blacklistRecordFields, Function.identity());
        Set r2 = allowDifferentSchema
            ? toSetWithBlacklistedFields(it2, blacklistRecordFields, GenericRecordWrapper::new)
            : toSetWithBlacklistedFields(it2, blacklistRecordFields, Function.identity());
        if (r1.equals(r2)) {
          return true;
        } else {
          log.info("Sets of records differ.");
          return false;
        }
      } else {
        return compareIterators(it1, it2, (r1, r2) -> {
          if (blacklistRecordFields != null) {
            for (String blacklisted : blacklistRecordFields) {
              r1.put(blacklisted, null);
              r2.put(blacklisted, null);
            }
          }
          return allowDifferentSchema ?
              GenericRecordWrapper.compareGenericRecordRegardlessOfSchema(r1, r2) : r1.equals(r2);
        });
      }
    });
  }

  private static <T> Set<T> toSetWithBlacklistedFields(Iterator<GenericRecord> it,
      Collection<String> blacklistRecordFields, Function<GenericRecord, T> transform) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false).map(r -> {
      for (String blacklisted : blacklistRecordFields) {
        r.put(blacklisted, null);
      }
      return transform.apply(r);
    }).collect(Collectors.toSet());
  }

  /**
   * Read all avro records in an HDFS location into a map from file name to {@link RecordIterator}.
   */
  @Override
  public TreeMap<String, RecordIterator> readAllRecordsInBinaryDirectory(FileSystem fs, Path path)
      throws IOException {
    TreeMap<String, RecordIterator> output = new TreeMap<>();
    if (!fs.exists(path)) {
      return output;
    }
    PathFilter pathFilter = new HiddenFilter();
    for (FileStatus status : FileListUtils.listFilesRecursively(fs, path, pathFilter)) {
      SeekableInput sin = new FsInput(status.getPath(), fs);
      DataFileReader<GenericRecord> dfr = new DataFileReader<>(sin, new GenericDatumReader<>());

      String key = PathUtils.relativizePath(status.getPath(), path).toString();

      output.put(key, new RecordIterator(dfr.getSchema(), new AbstractIterator<GenericRecord>() {
        @Override
        protected GenericRecord computeNext() {
          if (dfr.hasNext()) {
            return dfr.next();
          } else {
            try {
              dfr.close();
            } catch (IOException ioe) {
              log.error("Failed to close data file reader.", ioe);
            }
            endOfData();
            return null;
          }
        }
      }));
    }
    return output;
  }

  /**
   * Read all avro records in a json base resource in classpath into a map from file name to {@link RecordIterator}.
   */
  @Override
  public TreeMap<String, RecordIterator> readAllRecordsInJsonResource(String baseResource, @Nullable Schema schema)
      throws IOException {
    if (schema == null) {
      String schemaResource = new File(baseResource, "schema.avsc").toString();
      schema = readAvscSchema(schemaResource, AvroTestTools.class);
    }

    TreeMap<String, RecordIterator> output = new TreeMap<>();
    for (String file : getJsonFileSetByResourceRootName(baseResource)) {
      log.info("Reading json record from " + file);
      String name = PathUtils.relativizePath(new Path(file), new Path(baseResource)).toString();

      String schemaResourceName = new File(new File(file).getParent(), "schema.avsc").toString();
      Schema thisSchema = readAvscSchema(schemaResourceName, AvroTestTools.class);
      Schema actualSchema = thisSchema == null ? schema : thisSchema;

      try (InputStream is = AvroTestTools.class.getClassLoader().getResourceAsStream(file)) {
        output.put(name,
            readRecordsFromJsonInputStream(actualSchema, is, DecoderFactory.get().jsonDecoder(actualSchema, is)));
      }
    }
    return output;
  }

  private static RecordIterator readRecordsFromJsonInputStream(Schema schema, InputStream is, Decoder decoder) {
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

    return new RecordIterator(schema, new AbstractIterator<GenericRecord>() {
      @Override
      protected GenericRecord computeNext() {
        try {
          return reader.read(null, decoder);
        } catch (IOException ioe) {
          try {
            is.close();
          } catch (IOException exc) {
            log.warn("Failed to close input stream.", exc);
          }
          endOfData();
          return null;
        }
      }
    });
  }

  /**
   * Materialize records in a classpath package into HDFS avro records.
   * @param baseResource name of the package. The package should contain the following:
   *                     - Exactly one resource called <name>.avsc containing the schema of the records
   *                       (or an explicit schema passed as an argument).
   *                     - One or more data files called *.json containing the records.
   * @param fs the {@link FileSystem} where the records will be written.
   * @param targetPath the path where the records will be written.
   * @param schema Schema of the records, or null to read automatically from a resource.
   * @throws IOException
   */

  public Schema writeJsonResourceRecordsAsBinary(String baseResource, FileSystem fs, Path targetPath,
      @Nullable Schema schema) throws IOException {
    TreeMap<String, RecordIterator> recordMap = readAllRecordsInJsonResource(baseResource, schema);

    Schema outputSchema = recordMap.lastEntry().getValue().getSchema();

    for (Map.Entry<String, RecordIterator> entry : recordMap.entrySet()) {
      writeAsAvroBinary(entry.getValue(), entry.getValue().getSchema(), fs, new Path(targetPath,
          removeExtension(entry.getKey()) + ".avro"));
    }

    return outputSchema;
  }

  /**
   * Read schema from an avsc resource file.
   */
  public static Schema readAvscSchema(String resource, Class loadedClass) throws IOException {
    try (InputStream is = loadedClass.getClassLoader().getResourceAsStream(resource)) {
      return is != null ? new Schema.Parser().parse(is) : null;
    }
  }

  private void writeAsAvroBinary(Iterator<GenericRecord> input, Schema schema, FileSystem fs,
      Path outputPath) throws IOException {

    DataFileWriter writer = new DataFileWriter(new GenericDatumWriter());

    writer.create(schema, fs.create(outputPath, true));
    while (input.hasNext()) {
      writer.append(input.next());
    }
    writer.close();

    log.info("Successfully wrote avro file to path " + outputPath);
  }

  /**
   * An iterator over {@link GenericRecord} which is also aware of schema.
   */
  @AllArgsConstructor
  public static class RecordIterator implements Iterator<GenericRecord> {

    @Getter
    private final Schema schema;
    @Delegate
    private final Iterator<GenericRecord> it;
  }

  /**
   * A wrapper of {@link GenericRecord} when schema of record is not important in comparison.
   */
  @AllArgsConstructor
  public static class GenericRecordWrapper {
    public GenericRecord record;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      GenericRecordWrapper that = (GenericRecordWrapper) o;
      return compareGenericRecordRegardlessOfSchema(record, that.record);
    }

    @Override
    public int hashCode() {
      // Getting value object array
      int indexLen = record.getSchema().getFields().size();
      Object[] objArr = new Object[indexLen];
      for (int i = 0; i < indexLen; i++) {
        objArr[i] = record.get(i);
      }
      return Objects.hash(objArr);
    }

    /**
     * Compare two {@link GenericRecord} instance without considering their schema.
     * Useful when we want to compare two records by discarding some of fields like header.
     */
    static boolean compareGenericRecordRegardlessOfSchema(GenericRecord r1, GenericRecord r2) {
      List<Schema.Field> listOfFields1 = r1.getSchema().getFields();
      List<Schema.Field> listOfFields2 = r2.getSchema().getFields();

      if (listOfFields1.size() != listOfFields2.size()) {
        return false;
      }

      boolean result = true;
      for (int i = 0; i < listOfFields1.size(); i++) {
        result = result && (
            ((r1.get(i) == null && r2.get(i) == null)
                || (listOfFields1.get(i).name().equals(listOfFields2.get(i).name())
                && (r1.get(i).equals(r2.get(i)))))
        );
      }

      return result;
    }
  }

  // Package-private methods shared by different format's tool-kit.
  static String removeExtension(String string) {
    if (string.endsWith(".avro") || string.endsWith(".json")) {
      return string.substring(0, string.length() - 5);
    }
    throw new IllegalArgumentException("Only support avro and json extensions.");
  }

  static Set<String> getJsonFileSetByResourceRootName(String baseResource) {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .forPackages(baseResource)
        .filterInputsBy(name -> name.startsWith(baseResource))
        .setScanners(new ResourcesScanner()));

    return reflections.getResources(url -> url.endsWith(".json"));
  }

  public static boolean isResourceExisted(String resource) throws IOException {
    return AvroTestTools.class.getClassLoader().getResource(resource) != null;
  }
}
