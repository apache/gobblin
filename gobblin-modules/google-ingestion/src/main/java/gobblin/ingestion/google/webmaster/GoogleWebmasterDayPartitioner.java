package gobblin.ingestion.google.webmaster;

import gobblin.configuration.State;
import gobblin.writer.partitioner.WriterPartitioner;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;


/**
 * Partition the output by the date of fetched data set
 */
public class GoogleWebmasterDayPartitioner implements WriterPartitioner<GenericRecord> {
  /**
   * Prepend a prefix to each partition
   */
  public static final String KEY_PARTITIONER_PREFIX = "writer.partitioner.google_webmasters.prefix";
  /**
   * Determine whether to include column names into the partition path
   */
  public static final String KEY_INCLUDE_COLUMN_NAMES = "writer.partitioner.google_webmasters.column_names.include";

  private static final String PARTITION_COLUMN_PREFIX = "type";
  private static final String PARTITION_COLUMN_YEAR = "year";
  private static final String PARTITION_COLUMN_MONTH = "month";
  private static final String PARTITION_COLUMN_DAY = "day";
  private static final String DATE_COLUMN = "Date";

  private final String _prefix;
  private final boolean _withPrefix;
  private final Schema _partitionSchema;
  private final boolean _withColumnNames;

  public GoogleWebmasterDayPartitioner(State state, int numBranches, int branchId) {
    _withColumnNames = state.getPropAsBoolean(KEY_INCLUDE_COLUMN_NAMES);
    _prefix = state.getProp(KEY_PARTITIONER_PREFIX);
    _withPrefix = _prefix != null && !_prefix.trim().equals("");

    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record("YearMonthDayPartitioner").namespace("gobblin.ingestion.google.webmaster").fields();
    Schema stringType = Schema.create(Schema.Type.STRING);

    if (_withPrefix) {
      assembler = assembler.name(PARTITION_COLUMN_PREFIX).type(stringType).noDefault();
    }
    _partitionSchema = assembler.name(PARTITION_COLUMN_YEAR)
        .type(stringType)
        .noDefault()
        .name(PARTITION_COLUMN_MONTH)
        .type(stringType)
        .noDefault()
        .name(PARTITION_COLUMN_DAY)
        .type(stringType)
        .noDefault()
        .endRecord();
  }

  @Override
  public Schema partitionSchema() {
    return _partitionSchema;
  }

  @Override
  public GenericRecord partitionForRecord(GenericRecord record) {
    GenericRecord partition = new GenericData.Record(_partitionSchema);
    String dateString = record.get(DATE_COLUMN).toString();
    DateTime date = GoogleWebmasterExtractor.dateFormatter.parseDateTime(dateString);

    if (_withPrefix) {
      if (_withColumnNames) {
        partition.put(PARTITION_COLUMN_PREFIX, PARTITION_COLUMN_PREFIX + "=" + _prefix);
      } else {
        partition.put(PARTITION_COLUMN_PREFIX, _prefix);
      }
    }

    if (_withColumnNames) {
      partition.put(PARTITION_COLUMN_YEAR, PARTITION_COLUMN_YEAR + "=" + date.getYear());
      partition.put(PARTITION_COLUMN_MONTH, PARTITION_COLUMN_MONTH + "=" + date.getMonthOfYear());
      partition.put(PARTITION_COLUMN_DAY, PARTITION_COLUMN_DAY + "=" + date.getDayOfMonth());
    } else {
      partition.put(PARTITION_COLUMN_YEAR, date.getYear());
      partition.put(PARTITION_COLUMN_MONTH, date.getMonthOfYear());
      partition.put(PARTITION_COLUMN_DAY, date.getDayOfMonth());
    }

    return partition;
  }
}
