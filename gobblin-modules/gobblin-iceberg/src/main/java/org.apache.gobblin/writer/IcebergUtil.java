package org.apache.gobblin.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import java.util.Map;

public class IcebergUtil {

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
