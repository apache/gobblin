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
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;


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

  public AvroHdfsTimePartitionedWithRecordCountsWriter(Destination destination, String writerId, Schema schema,
      WriterOutputFormat writerOutputFormat, int numBranches, int branch) {
    super(destination, writerId, schema, writerOutputFormat, numBranches, branch);
  }

  @Override
  public void close() throws IOException {
    super.close();

    String fileName = WriterUtils.getWriterFileName(this.properties, this.numBranches, this.branch, this.writerId,
        this.writerOutputFormat.getExtension());

    Path writerOutputDir = new Path(this.properties.getProp(ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, this.numBranches, this.branch)));

    for (Entry<Path, DataWriter<GenericRecord>> entry : this.pathToWriterMap.entrySet()) {

      String filePathOld = new Path(entry.getKey(), fileName).toString();

      String filePathNew = filePathOld.substring(0, filePathOld.lastIndexOf(".")) + "."
          + entry.getValue().recordsWritten() + filePathOld.substring(filePathOld.lastIndexOf("."));

      HadoopUtils.renamePath(((AvroHdfsDataWriter) entry.getValue()).getFileSystem(),
          new Path(writerOutputDir, filePathOld), new Path(writerOutputDir, filePathNew));
    }
  }
}
