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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * A ToolKit that will be used for:
 * - Creating binary-format file(Avro, ORC) using records declared in txt(.json) file, and schema defined in .avsc file.
 * - Deserializing binary-format file into traversable in-memory objects.
 * - Verifying if contents in two binary-format file are identical with certain constraints.
 *
 *
 * @param <T> Iterator containing specific type of a record row,
 *           e.g. {@link org.apache.avro.generic.GenericRecord} for Avro.
 * @param <S> Schema type of a specific data format.
 */
@Slf4j
public abstract class DataTestTools<T, S> {
  /**
   * Verify that the two inputs contain the same records in the same file names. Any fields listed in
   * blacklistRecordFields will not be used for comparison.
   * Note that this method is destructive to the input records.
   * @param expected Expected records map, keyed by file name.
   * @param observed Observed records map, keyed by file name
   * @param allowDifferentOrder True if allowing fields arranged in different order in comparison of two records.
   * @param blacklistRecordFields Configurable set of fields that won't be included for comparison of two records.
   * @param allowDifferentSchema True if schema info (for avro, schema can contain attributes which is not necessary
   *                             to be included for comparison)
   * @return
   */
  public abstract boolean checkSameFilesAndRecords(TreeMap<String, T> expected, TreeMap<String, T> observed,
      boolean allowDifferentOrder, Collection<String> blacklistRecordFields, boolean allowDifferentSchema);

  /**
   * Write a resource file under a certain path as specified binary format file, like Avro, ORC.
   * @param baseResource Resource folder that contain JSON files.
   * @param fs
   * @param targetPath Output Path.
   * @param schema The schema of outputed binary file
   * @return
   * @throws IOException
   */
  public abstract S writeJsonResourceRecordsAsBinary(String baseResource, FileSystem fs, Path targetPath, S schema)
      throws IOException;

  /**
   * Read all records in a json base resource in classpath into a map from file name to iterator of T object.
   * @param baseResource Base path of the resource directory that contains json file.
   * @param schema The schema of records.
   * @return A map between file name to an iterator of objects contained in path.
   */
  public abstract TreeMap<String, T> readAllRecordsInJsonResource(String baseResource, S schema) throws IOException;

  /**
   * Read binary-format records into a map from file name to an iterator of T object.
   * @param fs File system object.
   * @param path File path
   * @return A map between file name to an iterator of objects contained in path.
   * @throws IOException
   */
  public abstract TreeMap<String, T> readAllRecordsInBinaryDirectory(FileSystem fs, Path path) throws IOException;

  /**
   * Compare two iterators in T type.
   */
  <T> boolean compareIterators(Iterator<T> expected, Iterator<T> observed, BiFunction<T, T, Boolean> comparator) {
    while (expected.hasNext()) {
      if (!observed.hasNext()) {
        log.error("Expected has more elements than observed.");
        return false;
      }

      T t1 = expected.next();
      T t2 = observed.next();

      boolean equals = comparator == null ? t1.equals(t2) : comparator.apply(t1, t2);

      if (!equals) {
        log.error(String.format("Mismatch: %s does not equal %s.", t1, t2));
        return false;
      }
    }

    if (observed.hasNext()) {
      log.error("Observed has more elements than expected.");
      return false;
    }

    return true;
  }
}
