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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class AvroHdfsDatePartitionedWriter implements DataWriter<GenericRecord> {

  private final String writerPartitionColumn;

  private final Map<Path, AvroHdfsDataWriter> pathToWriterMap = Maps.newHashMap();

  private final Destination destination;
  private final String writerId;
  private final Schema schema;
  private final WriterOutputFormat writerOutputFormat;
  private final State properties;
  private final int branch;

  private final String baseFilePath;
  private final String writerFilePathKey;

  //Joda formatters
  private static final DateTimeFormatter DAILY_FOLDER_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd");

  public AvroHdfsDatePartitionedWriter(Destination destination, String writerId, Schema schema,
      WriterOutputFormat writerOutputFormat, int branch) {

    this.destination = destination;
    this.writerId = writerId;
    this.schema = schema;
    this.writerOutputFormat = writerOutputFormat;
    this.properties = destination.getProperties();
    this.branch = branch;

    this.writerFilePathKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, branch);

    this.baseFilePath = this.properties
        .getProp(this.writerFilePathKey);

    Preconditions.checkArgument(this.properties.contains(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_COLUMN, branch)));

    this.writerPartitionColumn = this.properties.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_COLUMN, branch));
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(GenericRecord record) throws IOException {
    Optional<Object> writerPartitionColumnValue = AvroUtils.getFieldValue(record, this.writerPartitionColumn);
    if (writerPartitionColumnValue.isPresent()) {
      Path path = getPath((Long) writerPartitionColumnValue.get());
      if (!pathToWriterMap.containsKey(path)) {

//        Destination newDest = Destination.of(this.destination.getType(), this.properties.setProp(this.writerFilePathKey, baseFilePath + / ));

        DataWriter<GenericRecord> obj = new AvroDataWriterBuilder().writeTo(this.destination)
            .writeInFormat(this.writerOutputFormat).withWriterId(this.writerId)
            .withSchema(this.schema).forBranch(this.branch).build();

//        this.pathToWriterMap.put(path, obj);
      }
      this.pathToWriterMap.get(path).write(record);
    }
  }

  @Override
  public void commit() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void cleanup() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public long recordsWritten() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  private Path getPath(long timestamp) {
    return new Path(DAILY_FOLDER_FORMATTER.print(timestamp));
  }
}
