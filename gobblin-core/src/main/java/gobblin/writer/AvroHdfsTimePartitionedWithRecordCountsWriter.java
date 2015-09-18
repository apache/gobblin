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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.HadoopUtils;


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

      String filePathNew =
          new Path(new Path(filePathOld).getParent(), Files.getNameWithoutExtension(filePathOld)).toString() + "."
              + entry.getValue().recordsWritten() + "." + Files.getFileExtension(filePathOld);

      this.properties.appendToListProp(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, filePathNew);

      LOG.info("Renaming " + filePathOld + " to " + filePathNew);
      HadoopUtils.renamePath(((AvroHdfsDataWriter) entry.getValue()).getFileSystem(), new Path(filePathOld),
          new Path(filePathNew));
    }
  }
}
