package gobblin.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;
import gobblin.util.ForkOperatorUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class AvroHdfsDatePartitionedWriter implements DataWriter<GenericRecord> {

  private final String writerPartitionColumn;

  private final Map<Path, DataWriter<GenericRecord>> pathToWriterMap = Maps.newHashMap();

  private final Destination destination;
  private final String writerId;
  private final Schema schema;
  private final WriterOutputFormat writerOutputFormat;
  private final State properties;
  private final int branch;

  private final String baseFilePath;
//  private final String writerFilePathKey;

  private static final String DATE_PARTITIONED_NAME = "daily";

  //Joda formatters
  private static final DateTimeFormatter DAILY_FOLDER_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd");

  private static final Logger LOG = LoggerFactory.getLogger(AvroHdfsDatePartitionedWriter.class);

  public AvroHdfsDatePartitionedWriter(Destination destination, String writerId, Schema schema,
      WriterOutputFormat writerOutputFormat, int branch) {

    LOG.info("Constructing new writer");

    this.destination = destination;
    this.writerId = writerId;
    this.schema = schema;
    this.writerOutputFormat = writerOutputFormat;
    this.properties = destination.getProperties();
    this.branch = branch;

    Preconditions.checkNotNull(this.branch);
    Preconditions.checkNotNull(this.properties);

    this.baseFilePath = this.properties
        .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, branch));

    Preconditions.checkArgument(this.properties.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_COLUMN, this.branch)));
    this.writerPartitionColumn = this.properties.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_COLUMN, this.branch));

    Optional<Schema> writerPartitionColumnSchema = AvroUtils.getFieldSchema(this.schema, this.writerPartitionColumn);

    Preconditions.checkArgument(writerPartitionColumnSchema.isPresent(), "The input schema to the Writer does not contain the field " + this.writerPartitionColumn);
    Preconditions.checkArgument(writerPartitionColumnSchema.get().getType().equals(Schema.Type.LONG), "The column " + this.writerPartitionColumn + " specified by " + ConfigurationKeys.WRITER_PARTITION_COLUMN + " must be of type " + Schema.Type.LONG);
  }

  @Override
  public void close() throws IOException {
    for (DataWriter<GenericRecord> dataWriter : this.pathToWriterMap.values()) {
      dataWriter.close();
    }
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    Optional<Object> writerPartitionColumnValue = AvroUtils.getFieldValue(record, this.writerPartitionColumn);

    if (writerPartitionColumnValue.isPresent()) {

      Path path = getPathForColumnValue((Long) writerPartitionColumnValue.get());

      if (!this.pathToWriterMap.containsKey(path)) {

        this.properties.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, this.branch), path.toString());

        Destination newDest = Destination.of(this.destination.getType(), this.properties);

        DataWriter<GenericRecord> avroHdfsDataWriter = new AvroDataWriterBuilder().writeTo(newDest)
            .writeInFormat(this.writerOutputFormat).withWriterId(this.writerId)
            .withSchema(this.schema).forBranch(this.branch).build();

        avroHdfsDataWriter.write(record);
        this.pathToWriterMap.put(path, avroHdfsDataWriter);
      } else {
        this.pathToWriterMap.get(path).write(record);
      }
    }
  }

  @Override
  public void commit() throws IOException {
    for (DataWriter<GenericRecord> dataWriter : this.pathToWriterMap.values()) {
      dataWriter.commit();
    }
  }

  @Override
  public void cleanup() throws IOException {
    for (DataWriter<GenericRecord> dataWriter : this.pathToWriterMap.values()) {
      dataWriter.cleanup();
    }
  }

  @Override
  public long recordsWritten() {
    long recordsWritten = 0;
    for (DataWriter<GenericRecord> dataWriter : this.pathToWriterMap.values()) {
      recordsWritten += dataWriter.recordsWritten();
    }
    return recordsWritten;
  }

  @Override
  public long bytesWritten() throws IOException {
    long bytesWritten = 0;
    for (DataWriter<GenericRecord> dataWriter : this.pathToWriterMap.values()) {
      bytesWritten += dataWriter.bytesWritten();
    }
    return bytesWritten;
  }

  private Path getPathForColumnValue(long timestamp) {
    return new Path(this.baseFilePath, DATE_PARTITIONED_NAME + Path.SEPARATOR + DAILY_FOLDER_FORMATTER.print(timestamp));
  }
}
