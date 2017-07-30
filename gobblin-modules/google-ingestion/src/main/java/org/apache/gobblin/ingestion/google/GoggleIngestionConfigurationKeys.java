package gobblin.ingestion.google;

public class GoggleIngestionConfigurationKeys {
  public static final String DAY_PARTITIONER_KEY_PREFIX = "writer.partitioner.google_ingestion";
  /**
   * Optional. Default to String.Empty
   * Prepend a prefix to each partition
   */
  public static final String KEY_PARTITIONER_PREFIX = DAY_PARTITIONER_KEY_PREFIX + "prefix";
  /**
   * Optional. Default to false.
   * Determine whether to include column names into the partition path.
   */
  public static final String KEY_INCLUDE_COLUMN_NAMES = DAY_PARTITIONER_KEY_PREFIX + "column_names.include";
  /**
   * Optional. Default to "Date".
   * Configure the column name for "Date" field/column.
   */
  public static final String KEY_DATE_COLUMN_NAME = DAY_PARTITIONER_KEY_PREFIX + "date.column_name";
  /**
   * Optional. Default to "yyyy-MM-dd".
   * Configure the date string format for date value in records
   */
  public static final String KEY_DATE_FORMAT = DAY_PARTITIONER_KEY_PREFIX + "date.format";

  /**
   * Configure the size of underlying blocking queue of the asynchronized iterator - AsyncIteratorWithDataSink
   * Default to 2000.
   */
  public static final String SOURCE_ASYNC_ITERATOR_BLOCKING_QUEUE_SIZE = "source.async_iterator.blocking_queue_size";

  /**
   * Configure the poll blocking time of underlying blocking queue of the asynchronized iterator - AsyncIteratorWithDataSink
   * Default to 1 second.
   */
  public static final String SOURCE_ASYNC_ITERATOR_POLL_BLOCKING_TIME = "source.async_iterator.poll_blocking_time";
}
