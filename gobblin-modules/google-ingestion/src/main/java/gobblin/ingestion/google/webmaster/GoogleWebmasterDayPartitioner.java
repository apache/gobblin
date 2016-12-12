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

  private static final String PARTITION_COLUMN_YEAR = "year";
  private static final String PARTITION_COLUMN_MONTH = "month";
  private static final String PARTITION_COLUMN_DAY = "day";
  private static final String DATE_COLUMN = "Date";

  private static final Schema SCHEMA = SchemaBuilder.record("YearMonthDayPartitioner")
      .namespace("gobblin.ingestion.google.webmaster")
      .fields()
      .name(PARTITION_COLUMN_YEAR)
      .type(Schema.create(Schema.Type.STRING))
      .noDefault()
      .name(PARTITION_COLUMN_MONTH)
      .type(Schema.create(Schema.Type.STRING))
      .noDefault()
      .name(PARTITION_COLUMN_DAY)
      .type(Schema.create(Schema.Type.STRING))
      .noDefault()
      .endRecord();

  public GoogleWebmasterDayPartitioner(State state, int numBranches, int branchId) {

  }

  @Override
  public Schema partitionSchema() {
    return SCHEMA;
  }

  @Override
  public GenericRecord partitionForRecord(GenericRecord record) {
    GenericRecord partition = new GenericData.Record(SCHEMA);
    String dateString = record.get(DATE_COLUMN).toString();
    DateTime date = GoogleWebmasterExtractor.dateFormatter.parseDateTime(dateString);

    partition.put(PARTITION_COLUMN_YEAR, "year=" + date.getYear());
    partition.put(PARTITION_COLUMN_MONTH, "month=" + date.getMonthOfYear());
    partition.put(PARTITION_COLUMN_DAY, "day=" + date.getDayOfMonth());

    return partition;
  }
}
