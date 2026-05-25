package org.apache.gobblin.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;


/**
 * Factory to create a new {@link FileAppender} that
 * write generic data. The created {@link FileAppender}
 * will be wrapped in {@link TaskWriter}
 */
public class IcebergFileAppenderFactory implements FileAppenderFactory {
  private final Schema schema;
  private final Map<String, String> config = Maps.newHashMap();

  public IcebergFileAppenderFactory(Schema schema) {
    this.schema = schema;
  }

  public IcebergFileAppenderFactory set(String property, String value) {
    config.put(property, value);
    return this;
  }

  public IcebergFileAppenderFactory setAll(Map<String, String> properties) {
    config.putAll(properties);
    return this;
  }

  @Override
  public FileAppender newAppender(OutputFile outputFile, FileFormat fileFormat) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
    try {
      switch (fileFormat) {
        case AVRO:
          return Avro.write(outputFile)
              .schema(schema)
              // No writerFunc is specified here, so a GenericAvroWriter::new
              // will be added implicitly.
              // To explicitly add, use .createWriterFunc()
              .setAll(config)
              .overwrite()
              .build();

        case PARQUET:
          return Parquet.write(outputFile)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .setAll(config)
              .metricsConfig(metricsConfig)
              .overwrite()
              .build();

        case ORC:
          return ORC.write(outputFile)
              .schema(schema)
              .createWriterFunc(GenericOrcWriter::buildWriter)
              .setAll(config)
              .overwrite()
              .build();

        default:
          throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
