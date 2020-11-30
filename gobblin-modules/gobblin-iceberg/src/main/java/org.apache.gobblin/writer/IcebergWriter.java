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
package org.apache.gobblin.writer;

import org.apache.gobblin.configuration.State;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.TaskWriter;

import java.io.IOException;
import java.util.Map;


/**
 * An extension to {@link FsDataWriter} that writes in Iceberg formats.
 *
 * <p>
 *   This implementation allows users to specify different formats
 *   through {@link TaskWriter} to write data. The {@link TaskWriter} will
 *   be created through {@link IcebergTaskWriterFactory}.
 * </p>
 *
 */
public class IcebergWriter<D> extends FsDataWriter<D>{

    private final TaskWriter<D> writer;

    public IcebergWriter(IcebergDataWriterBuilder builder, State properties)
            throws IOException {
        super(builder, properties);

        Table table = createTable(outputFile.toUri().toString(), (Map)properties.getProperties(), builder.partition.isPresent(), builder.schema);
        IcebergTaskWriterFactory taskWriterFactory = new IcebergTaskWriterFactory(builder.schema, table.spec(), table.locationProvider(), table.io(), table.encryption(),
                this.blockSize, formatConvertor(builder.format), table.properties());
        taskWriterFactory.initialize(Integer.parseInt(builder.writerId), 0);
        this.writer = taskWriterFactory.create();
    }

    @Override
    public long recordsWritten() {
        return 0;
    }

    @Override
    public void write(D record)
            throws IOException {
        this.writer.write(record);
    }

    public static Table createTable(String path, Map<String, String> properties, boolean partitioned, Schema schema) {
        PartitionSpec spec;
        if (partitioned) {
            spec = PartitionSpec.builderFor(schema).identity("data").build();
        } else {
            spec = PartitionSpec.unpartitioned();
        }
        return new HadoopTables().create(schema, spec, properties, path);
    }

    public static FileFormat formatConvertor(WriterOutputFormat format) {
        switch (format) {
            case AVRO:
                return FileFormat.AVRO;
            case ORC:
                return FileFormat.ORC;
            case PARQUET:
                return FileFormat.PARQUET;
            default:
                throw new RuntimeException("Unknown File format : " + format);

        }
    }
}
