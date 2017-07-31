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
package org.apache.gobblin.audit.values.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import lombok.Getter;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Closer;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.audit.values.auditor.ValueAuditRuntimeMetadata;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


/**
 *  A Hadoop {@link FileSystem} based {@link AuditSink} that writes audit {@link GenericRecord}s to a file on {@link FileSystem}.
 *  <ul>
 *  <li> The {@link FileSystem} {@link URI} can be set using key {@link ConfigurationKeys#FS_URI_KEY}, {@link ConfigurationKeys#LOCAL_FS_URI} is used by default.
 *  <li> All audit files are written under the base path. The base path can be set using key {@link #FS_SINK_AUDIT_OUTPUT_PATH_KEY}.
 *  The default path is,
 *  <pre>
 *  <code>System.getProperty("user.dir") + "/lumos_value_audit/local_audit";</code>
 *  </pre>
 *  <li> It uses <code>auditMetadata</code> to build the audit file name and path.<br>
 *  <b>The layout on {@link FileSystem} - </b>
 *  <pre>
 *  |-- &lt;Database&gt;
 *    |-- &lt;Table&gt;
 *       |-- P=&lt;PHASE&gt;.C=&lt;CLUSTER&gt;.E=&lt;EXTRACT_ID&gt;.S=&lt;SNAPSHOT_ID&gt;.D=&lt;DELTA_ID&gt;
 *          |-- *.avro
 *  </pre>
 *  </ul>
 */
@Alias(value = "FsAuditSink")
public class FsAuditSink implements AuditSink {

  private static final String FS_SINK_AUDIT_OUTPUT_PATH_KEY = "fs.outputDirPath";
  private static final String FS_SINK_AUDIT_OUTPUT_DEFAULT_PATH = System.getProperty("user.dir") + "/lumos_value_audit/local_audit";
  private static final String FILE_NAME_DELIMITTER = "_";

  private final FileSystem fs;
  private final OutputStream auditFileOutputStream;
  private final DataFileWriter<GenericRecord> writer;
  private final Closer closer = Closer.create();
  private final ValueAuditRuntimeMetadata auditMetadata;
  @Getter
  private final Path auditDirPath;

  public FsAuditSink(Config config, ValueAuditRuntimeMetadata auditMetadata) throws IOException {

    this.auditDirPath = new Path(ConfigUtils.getString(config, FS_SINK_AUDIT_OUTPUT_PATH_KEY, FS_SINK_AUDIT_OUTPUT_DEFAULT_PATH));
    this.fs = this.auditDirPath.getFileSystem(new Configuration());
    this.auditMetadata = auditMetadata;
    this.auditFileOutputStream = closer.register(fs.create(getAuditFilePath()));
    DataFileWriter<GenericRecord> dataFileWriter = this.closer.register(new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>()));
    this.writer = this.closer.register(dataFileWriter.create(this.auditMetadata.getTableMetadata().getTableSchema(), this.auditFileOutputStream));
  }

  /**
   * Returns the complete path of the audit file. Generate the audit file path with format
   *
   *  <pre>
   *  |-- &lt;Database&gt;
   *    |-- &lt;Table&gt;
   *       |-- P=&lt;PHASE&gt;.C=&lt;CLUSTER&gt;.E=&lt;EXTRACT_ID&gt;.S=&lt;SNAPSHOT_ID&gt;.D=&lt;DELTA_ID&gt;
   *          |-- *.avro
   *  </pre>
   *
   */
  public Path getAuditFilePath() {
    StringBuilder auditFileNameBuilder = new StringBuilder();
    auditFileNameBuilder.append("P=").append(auditMetadata.getPhase()).append(FILE_NAME_DELIMITTER).append("C=")
        .append(auditMetadata.getCluster()).append(FILE_NAME_DELIMITTER).append("E=")
        .append(auditMetadata.getExtractId()).append(FILE_NAME_DELIMITTER).append("S=")
        .append(auditMetadata.getSnapshotId()).append(FILE_NAME_DELIMITTER).append("D=")
        .append(auditMetadata.getDeltaId());

    return new Path(auditDirPath, PathUtils.combinePaths(auditMetadata.getTableMetadata().getDatabase(), auditMetadata
        .getTableMetadata().getTable(), auditFileNameBuilder.toString(), auditMetadata.getPartFileName()));
  }

  /**
   * Append this record to the {@link DataFileWriter}
   *
   * {@inheritDoc}
   * @see org.apache.gobblin.audit.values.sink.AuditSink#write(org.apache.avro.generic.GenericRecord)
   */
  @Override
  public void write(GenericRecord record) throws IOException {
    this.writer.append(record);
  }

  @Override
  public final void close() throws IOException {
    this.closer.close();
  }
}
