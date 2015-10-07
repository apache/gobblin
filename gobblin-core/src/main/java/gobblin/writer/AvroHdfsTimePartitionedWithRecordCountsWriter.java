/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.HadoopUtils;
import gobblin.util.RecordCountProvider;


/**
 * Implementation of {@link DataWriter} that writes data into a date-partitioned directory structure based on the value
 * of a specific field in each Avro record, while also including in the name of each file the number of records
 * contained within that file.
 *
 * See {@link AvroHdfsTimePartitionedWriter} for more details on complete output path. This DataWriter maintains
 * the same behavior except for including the number of records in the file name immediately before the extension.
 * Where AvroHdfsTimePartitionedWriter would output path/fileName.avro,
 * AvroHdfsTimePartitionedWithRecordCountsWriter would output path/fileName.{recordCount}.avro, e.g.
 * path/fileName.450.avro
 *
 * This Writer behaves exactly as the AvroHdfsTimePartitionedWriter, but renames the outputted Avro files after
 * they are closed.
 */
public class AvroHdfsTimePartitionedWithRecordCountsWriter extends AvroHdfsTimePartitionedWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AvroHdfsTimePartitionedWithRecordCountsWriter.class);

  public AvroHdfsTimePartitionedWithRecordCountsWriter(Destination destination, String writerId, Schema schema,
      WriterOutputFormat writerOutputFormat, int numBranches, int branch) {
    super(destination, writerId, schema, writerOutputFormat, numBranches, branch);
  }

  @Override
  public void close() throws IOException {
    super.close();

    // Rewrite property writer.final.output.file.paths due to renaming output files.
    this.properties.removeProp(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS);

    for (Entry<Path, FsDataWriter<GenericRecord>> entry : this.pathToWriterMap.entrySet()) {

      String filePathOld = entry.getValue().getOutputFilePath();

      String filePathNew = new FilenameRecordCountProvider().constructFilePath(filePathOld, entry.getValue().recordsWritten());

      this.properties.appendToListProp(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, filePathNew);

      LOG.info("Renaming " + filePathOld + " to " + filePathNew);
      HadoopUtils.renamePath(((AvroHdfsDataWriter) entry.getValue()).getFileSystem(), new Path(filePathOld), new Path(
          filePathNew));
    }
  }

  /**
   * Implementation of {@link RecordCountProvider}, which provides record count from file path.
   * The file path should follow the pattern: {Filename}.{RecordCount}.{Extension}.
   * For example, given a file path: "/a/b/c/file.123.avro", the record count will be 123.
   */
  public static class FilenameRecordCountProvider implements RecordCountProvider {
    private static final String SEPARATOR = ".";

    /**
     * Construct a new file path by appending record count to the filename of the given file path, separated by SEPARATOR.
     * For example, given path: "/a/b/c/file.avro" and record count: 123,
     * the new path returned will be: "/a/b/c/file.123.avro"
     */
    public String constructFilePath(String oldFilePath, long recordCounts) {
      return new Path(new Path(oldFilePath).getParent(), Files.getNameWithoutExtension(oldFilePath).toString()
          + SEPARATOR + recordCounts + SEPARATOR + Files.getFileExtension(oldFilePath)).toString();
    }

    /**
     * The record count should be the last component before the filename extension.
     */
    @Override
    public long getRecordCount(Path filepath) {
      String[] components = filepath.getName().split(Pattern.quote(SEPARATOR));
      Preconditions.checkArgument(components.length >= 2 && StringUtils.isNumeric(components[components.length - 2]),
          String.format("Filename %s does not follow the pattern: FILENAME.RECORDCOUNT.EXTENSION", filepath));
      return Long.parseLong(components[components.length - 2]);
    }
  }
}
