package org.apache.gobblin.writer;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.TaskWriter;

public class TestIcebergWriter {

    private Table table;

    private TaskWriter<Record> createTaskWriter(long targetFileSize) {
        return null;
        //return taskWriterFactory.create();
    }
}
