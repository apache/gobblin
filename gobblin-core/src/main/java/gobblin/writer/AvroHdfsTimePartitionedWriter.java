package gobblin.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;
import gobblin.util.ForkOperatorUtils;


/**
 * Implementation of {@link DataWriter} that writes data into a date-partitioned directory structure based on the value
 * of a specific field in each Avro record.
 *
 * <p>
 *
 * The writer takes in each Avro record, extracts out a specific field, and based on the the value of the column it
 * writes the data into a specific file. The column is assumed to be of type {@link Long}, and the value is treated as
 * a timestamp. By default, the timestamp is converted into a directory of the form [yyyy]/[MM][dd]/. However, the
 * actual pattern used to construct the path is specified by {@link ConfigurationKeys#WRITER_PARTITION_PATTERN}. This
 * class uses Joda's {@link DateTimeFormatter} to parse a timestamp, and convert it to a {@link String}. The
 * {@link String} is then converted to a Path, which specifies where the record will be written.
 *
 * <p>
 *
 * By default, The complete output path for a directory is "/baseFilePath/daily/[yyyy]/[MM]/[dd]/fileName.avro". The
 * baseFilePath is specified by the configuration key {@link ConfigurationKeys#WRITER_FILE_PATH}. The writer uses the
 * configuration key {@link ConfigurationKeys#WRITER_PARTITION_COLUMN_NAME} to determine the name of the column to
 * partition by. The "daily" portion of the path is also configurable, and can be changed by setting
 * {@link ConfigurationKeys#DEFAULT_WRITER_PARTITION_LEVEL}.
 *
 * <p>
 *
 * The class works by maintaining a {@link Map} of {@link Path}s to {@link DataWriter}s called {@link #pathToWriterMap}.
 * The {@link AvroHdfsTimePartitionedWriter#write(GenericRecord)} takes each record, extracts out the the value of the
 * field specified by {@link ConfigurationKeys#WRITER_PARTITION_COLUMN_NAME}, and constructs a {@link Path} based on the
 * field value. If a {@link DataWriter} already exists for the path, the record is simply written to the existing
 * {@link DataWriter}. If one does not exist, then a new {@link DataWriter} is created, added to the Map, and then the
 * input record is written to the writer.
 *
 * <p>
 *
 * By using the above approach methods such as {@link DataWriter#commit()}, {@link DataWriter#close()}, etc. are simple.
 * The implementation is to simply iterate over the values of {@link #pathToWriterMap} and call the corresponding method
 * on each {@link DataWriter}.
 */
public class AvroHdfsTimePartitionedWriter implements DataWriter<GenericRecord> {

  /**
   * This is the base file path that all data will be written to. By default, data will be written to
   * /baseFilePath/daily/[yyyy]/[MM]/[dd]/.
   */
  private final String baseFilePath;

  /**
   * The name of the column that the writer will use to partition the data.
   */
  private final String partitionColumnName;

  /**
   * The name that separates the {@link #baseFilePath} from the path created by the {@link #timestampToPathFormatter}.
   * The default value is specified by {@link ConfigurationKeys#DEFAULT_WRITER_PARTITION_LEVEL}.
   */
  private final String partitionLevel;

  /**
   * This {@link DateTimeFormatter} controls how the date-partitioned directories will be created. The default pattern
   * used is specified by {@link ConfigurationKeys#DEFAULT_WRITER_PARTITION_PATTERN}. The
   * {@link DateTimeFormatter#print(long)} method is used to convert a timestamp to a {@link String}.
   */
  private final DateTimeFormatter timestampToPathFormatter;

  /**
   * Maps a {@link Path} to the the {@link DataWriter} that is writing data to the Path.
   */
  private final Map<Path, DataWriter<GenericRecord>> pathToWriterMap = Maps.newHashMap();

  // Variables needed to build DataWriters
  private final Destination destination;
  private final String writerId;
  private final Schema schema;
  private final WriterOutputFormat writerOutputFormat;
  private final State properties;
  private final int branch;

  private static final Logger LOG = LoggerFactory.getLogger(AvroHdfsTimePartitionedWriter.class);

  public AvroHdfsTimePartitionedWriter(Destination destination, String writerId, Schema schema,
      WriterOutputFormat writerOutputFormat, int branch) {

    this.destination = destination;
    this.writerId = writerId;
    this.schema = schema;
    this.writerOutputFormat = writerOutputFormat;
    this.branch = branch;
    this.properties = destination.getProperties();

    // Confirm that all input parameters are not null
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkNotNull(this.writerId);
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkNotNull(this.writerOutputFormat);
    Preconditions.checkNotNull(this.branch);
    Preconditions.checkNotNull(this.properties);

    this.baseFilePath = this.properties.getProp(getWriterFilePath(this.branch));

    // Initialize the partitionLevel
    this.partitionLevel =
        this.properties.getProp(getWriterPartitionLevel(this.branch), ConfigurationKeys.DEFAULT_WRITER_PARTITION_LEVEL);

    // Initialize the timestampToPathFormatter
    this.timestampToPathFormatter =
        DateTimeFormat.forPattern(
            this.properties.getProp(getWriterPartitionPattern(this.branch),
                ConfigurationKeys.DEFAULT_WRITER_PARTITION_PATTERN)).withZone(
            DateTimeZone.forID(this.properties.getProp(ConfigurationKeys.WRITER_PARTITION_TIMEZONE,
                ConfigurationKeys.DEFAULT_WRITER_PARTITION_TIMEZONE)));

    // Check that ConfigurationKeys.WRITER_PARTITION_COLUMN_NAME has been specified and is properly formed

    Preconditions.checkArgument(this.properties.contains(getWriterPartitionColumnName(this.branch)),
        "Missing required property " + ConfigurationKeys.WRITER_PARTITION_COLUMN_NAME);

    this.partitionColumnName = this.properties.getProp(getWriterPartitionColumnName(this.branch));
    Optional<Schema> writerPartitionColumnSchema = AvroUtils.getFieldSchema(this.schema, this.partitionColumnName);

    Preconditions.checkArgument(writerPartitionColumnSchema.isPresent(), "The column " + this.partitionColumnName
        + " specified by " + ConfigurationKeys.WRITER_PARTITION_COLUMN_NAME + " is not in the writer input schema");

    Preconditions.checkArgument(writerPartitionColumnSchema.get().getType().equals(Schema.Type.LONG), "The column "
        + this.partitionColumnName + " specified by " + ConfigurationKeys.WRITER_PARTITION_COLUMN_NAME
        + " must be of type " + Schema.Type.LONG);
  }

  @Override
  public void write(GenericRecord record) throws IOException {

    // Retrieve the value of the field specified by this.partitionColumnName
    Optional<Object> writerPartitionColumnValue = AvroUtils.getFieldValue(record, this.partitionColumnName);
    assert writerPartitionColumnValue.isPresent();

    Path writerOutputPath = getPathForColumnValue((Long) writerPartitionColumnValue.get());

    // If the path is not in pathToWriterMap, construct a new DataWriter, add it to the map, and write the record
    // If the path is in pathToWriterMap simply retrieve the writer, and write the record
    if (!this.pathToWriterMap.containsKey(writerOutputPath)) {

      LOG.info("Creating a new DataWriter for path: " + new Path(this.baseFilePath, writerOutputPath));

      DataWriter<GenericRecord> avroHdfsDataWriter = createAvroHdfsDataWriterForPath(writerOutputPath);

      this.pathToWriterMap.put(writerOutputPath, avroHdfsDataWriter);
      avroHdfsDataWriter.write(record);
    } else {
      this.pathToWriterMap.get(writerOutputPath).write(record);
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

  @Override
  public void close() throws IOException {
    for (DataWriter<GenericRecord> dataWriter : this.pathToWriterMap.values()) {
      dataWriter.close();
    }
  }

  /**
   * Helper method to create a new {@link DataWriter} that will write to a specific {@link Path}. The new
   * {@link DataWriter} will have all the same properties use to construct {@link AvroHdfsTimePartitionedWriter}, except
   * the {@link ConfigurationKeys#WRITER_FILE_PATH} will be different.
   * @param path that the new {@link DataWriter} will write to.
   * @return a new {@link DataWriter} configured to write to the specified path.
   * @throws IOException if there is an problem creating the new {@link DataWriter}.
   */
  private DataWriter<GenericRecord> createAvroHdfsDataWriterForPath(Path path) throws IOException {

    // Create a copy of the properties object
    State state = new State();
    for (String key : this.properties.getPropertyNames()) {
      state.setProp(key, this.properties.getProp(key));
    }

    // Set the output path that the DataWriter will write to
    state.setProp(getWriterFilePath(this.branch), path.toString());

    return new AvroDataWriterBuilder().writeTo(Destination.of(this.destination.getType(), state))
        .writeInFormat(this.writerOutputFormat).withWriterId(this.writerId).withSchema(this.schema)
        .forBranch(this.branch).build();
  }

  /**
   * Given a timestamp of type long, convert the timestamp to a {@link Path} using the {@link #timestampToPathFormatter}.
   * @param timestamp is the timestamp that needs to be converted to a path.
   * @return a {@link Path} based on the value of the timestamp.
   */
  private Path getPathForColumnValue(long timestamp) {
    return new Path(this.baseFilePath, partitionLevel + Path.SEPARATOR + timestampToPathFormatter.print(timestamp));
  }

  /**
   * Helper method to get the branched configuration key for {@link ConfigurationKeys#WRITER_PARTITION_COLUMN_NAME}.
   */
  private String getWriterPartitionColumnName(int branch) {
    return ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_COLUMN_NAME, branch);
  }

  /**
   * Helper method to get the branched configuration key for {@link ConfigurationKeys#WRITER_FILE_PATH}.
   */
  private String getWriterFilePath(int branch) {
    return ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, branch);
  }

  /**
   * Helper method to get the branched configuration key for {@link ConfigurationKeys#WRITER_PARTITION_LEVEL}.
   */
  private String getWriterPartitionLevel(int branch) {
    return ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_LEVEL, branch);
  }

  /**
   * Helper method to get the branched configuration key for {@link ConfigurationKeys#WRITER_PARTITION_PATTERN}.
   */
  private String getWriterPartitionPattern(int branch) {
    return ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_PATTERN, branch);
  }
}
