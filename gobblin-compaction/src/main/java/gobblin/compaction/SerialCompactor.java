/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import gobblin.hive.util.HiveJdbcConnector;


/**
 * An implementation of compactor. This class assumes that the snapshot table
 * and the delta tables are taken one after another (hence the name
 * "SerialCompactor").
 *
 * @author ziliu
 */
public class SerialCompactor implements Compactor {

  private static final Logger LOG = LoggerFactory.getLogger(SerialCompactor.class);

  private static final String HIVE_DB_NAME = "hive.db.name";
  private static final String HIVE_QUEUE_NAME = "hive.queue.name";
  private static final String HIVE_USE_MAPJOIN = "hive.use.mapjoin";
  private static final String HIVE_MAPJOIN_SMALLTABLE_FILESIZE = "hive.mapjoin.smalltable.filesize";
  private static final String HIVE_AUTO_CONVERT_JOIN = "hive.auto.convert.join";
  private static final String HIVE_INPUT_SPLIT_SIZE = "hive.input.split.size";
  private static final String MAPRED_MIN_SPLIT_SIZE = "mapred.min.split.size";
  private static final String MAPREDUCE_JOB_REDUCES = "mapreduce.job.reduces";
  private static final String MAPREDUCE_JOB_NUM_REDUCERS = "mapreduce.job.num.reducers";
  private static final String MAPREDUCE_JOB_QUEUENAME = "mapreduce.job.queuename";

  private final AvroExternalTable snapshot;
  private final List<AvroExternalTable> deltas;
  private final String outputTableName;
  private final String outputDataLocationInHdfs;
  private final AvroExternalTable latestTable;
  private final String jobId;
  private HiveJdbcConnector conn;

  public static class Builder {
    private AvroExternalTable snapshot;
    private List<AvroExternalTable> deltas;
    private String outputTableName;
    private String outputDataLocationInHdfs;

    public Builder withSnapshot(AvroExternalTable snapshot) {
      this.snapshot = snapshot;
      return this;
    }

    public Builder withDeltas(List<AvroExternalTable> deltas) {
      Preconditions.checkArgument(deltas.size() >= 1, "Number of delta tables should be at least 1");
      this.deltas = deltas;
      return this;
    }

    public Builder withOutputTableName(String outputTableName) {
      this.outputTableName = outputTableName;
      return this;
    }

    public Builder withOutputDataLocationInHdfs(String outputDataLocationInHdfs) {
      this.outputDataLocationInHdfs = outputDataLocationInHdfs;
      return this;
    }

    public SerialCompactor build() {
      return new SerialCompactor(this);
    }
  }

  private SerialCompactor(SerialCompactor.Builder builder) {
    this.snapshot = builder.snapshot;
    this.deltas = builder.deltas;
    this.outputTableName = builder.outputTableName;
    this.outputDataLocationInHdfs = builder.outputDataLocationInHdfs;
    this.latestTable = this.deltas.get(this.deltas.size() - 1);
    this.jobId = UUID.randomUUID().toString().replaceAll("-", "_");
  }

  @Override
  public void compact() throws IOException {

    checkSchemaCompatibility();

    Closer closer = Closer.create();

    try {
      this.conn =
          closer.register(HiveJdbcConnector.newConnectorWithProps(CompactionRunner.properties));

      setHiveParameters();
      createTables();
      HiveTable mergedDelta = mergeDeltas();
      HiveManagedTable notUpdated = getNotUpdatedRecords(this.snapshot, mergedDelta);
      unionNotUpdatedRecordsAndDeltas(notUpdated, mergedDelta);
    } catch (SQLException e) {
      LOG.error("SQLException during compaction: " + e.getMessage());
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("IOException during compaction: " + e.getMessage());
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      LOG.error("Runtime Exception during compaction: " + e.getMessage());
      throw e;
    } finally {
      try {
        deleteTmpFiles();
      } finally {
        closer.close();
      }
    }
  }

  private void checkSchemaCompatibility() {
    for (int i = 0; i < deltas.size(); i++) {
      if (!this.snapshot.hasSamePrimaryKey(this.deltas.get(i))) {
        String message =
            "Schema incompatible: the snapshot table and delta table #" + (i + 1)
                + " do not have the same primary key.";
        LOG.error(message);
        throw new RuntimeException(message);
      }
    }
  }

  private void setHiveParameters() throws SQLException {
    setHiveQueueName();
    setHiveDbName();
    setHiveMapjoin();
    setHiveInputSplitSize();
    setNumberOfReducers();
  }

  private void setHiveQueueName() throws SQLException {
    this.conn.executeStatements("set " + MAPREDUCE_JOB_QUEUENAME + "="
        + CompactionRunner.jobProperties.getProperty(HIVE_QUEUE_NAME, "default"));
  }

  private void setHiveDbName() throws SQLException {
    this.conn.executeStatements("use " + CompactionRunner.jobProperties.getProperty(HIVE_DB_NAME, "default"));
  }

  private void setHiveMapjoin() throws SQLException {
    boolean useMapjoin = Boolean.parseBoolean(CompactionRunner.jobProperties.getProperty(HIVE_USE_MAPJOIN, "false"));
    boolean smallTableSizeSpecified = CompactionRunner.jobProperties.containsKey(HIVE_MAPJOIN_SMALLTABLE_FILESIZE);

    if (useMapjoin && smallTableSizeSpecified) {
      this.conn.executeStatements("set " + HIVE_AUTO_CONVERT_JOIN + "=true");
      this.conn.executeStatements("set " + HIVE_MAPJOIN_SMALLTABLE_FILESIZE + "="
          + CompactionRunner.jobProperties.getProperty(HIVE_MAPJOIN_SMALLTABLE_FILESIZE));
    }
  }

  private void setHiveInputSplitSize() throws SQLException {
    boolean splitSizeSpecified = CompactionRunner.jobProperties.containsKey(HIVE_INPUT_SPLIT_SIZE);
    if (splitSizeSpecified) {
      this.conn.executeStatements("set " + MAPRED_MIN_SPLIT_SIZE + "="
          + CompactionRunner.jobProperties.getProperty(HIVE_INPUT_SPLIT_SIZE));
    }
  }

  private void setNumberOfReducers() throws SQLException {
    boolean numOfReducersSpecified = CompactionRunner.jobProperties.containsKey(MAPREDUCE_JOB_NUM_REDUCERS);

    if (numOfReducersSpecified) {
      this.conn.executeStatements("set " + MAPREDUCE_JOB_REDUCES + "="
          + CompactionRunner.jobProperties.getProperty(MAPREDUCE_JOB_NUM_REDUCERS));
    }
  }

  private void createTables() throws SQLException {
    this.snapshot.createTable(this.conn, this.jobId);

    for (AvroExternalTable delta : this.deltas) {
      delta.createTable(this.conn, this.jobId);
    }
  }

  private HiveTable mergeDeltas() throws SQLException {
    if (deltas.size() == 1) {
      LOG.info("Only one delta table: no need to merge delta");
      return this.deltas.get(0);
    }
    HiveManagedTable mergedDelta =
        new HiveManagedTable.Builder().withName("merged_delta").withAttributes(this.deltas.get(0).getAttributes())
            .withPrimaryKeys(this.deltas.get(0).getPrimaryKeys()).build();
    mergedDelta.createTable(this.conn, this.jobId);
    insertFirstDeltaIntoMergedDelta(mergedDelta);
    this.deltas.get(0).dropTable(this.conn, this.jobId);

    for (int i = 1; i < this.deltas.size(); i++) {
      mergedDelta = mergeTwoDeltas(mergedDelta, this.deltas.get(i));
      LOG.info("Merged the first " + (i + 1) + " delta tables");
      this.deltas.get(i).dropTable(this.conn, this.jobId);
    }
    return mergedDelta;
  }

  private void insertFirstDeltaIntoMergedDelta(HiveManagedTable mergedDelta) throws SQLException {
    String insertStmt =
        "INSERT OVERWRITE TABLE " + mergedDelta.getNameWithJobId(this.jobId) + " SELECT * FROM "
            + this.deltas.get(0).getNameWithJobId(this.jobId);
    this.conn.executeStatements(insertStmt);
  }

  private HiveManagedTable mergeTwoDeltas(HiveManagedTable mergedDelta, AvroExternalTable nextDelta)
      throws SQLException {
    HiveManagedTable notUpdated = getNotUpdatedRecords(mergedDelta, nextDelta);

    HiveTable notUpdatedWithNewSchema = notUpdated.addNewColumnsInSchema(this.conn, this.latestTable, this.jobId);
    HiveTable nextDeltaWithNewSchema = nextDelta.addNewColumnsInSchema(this.conn, this.latestTable, this.jobId);

    mergedDelta =
        new HiveManagedTable.Builder().withName(mergedDelta.getName()).withAttributes(this.latestTable.getAttributes())
            .withPrimaryKeys(this.latestTable.getPrimaryKeys()).build();

    mergedDelta.createTable(this.conn, this.jobId);

    String unionStmt =
        "INSERT OVERWRITE TABLE " + mergedDelta.getNameWithJobId(this.jobId) + " SELECT " + getAttributesInNewSchema()
            + " FROM " + notUpdatedWithNewSchema.getNameWithJobId(this.jobId) + " UNION ALL " + "SELECT "
            + getAttributesInNewSchema() + " FROM " + nextDeltaWithNewSchema.getNameWithJobId(this.jobId);
    conn.executeStatements(unionStmt);

    nextDelta.dropTable(this.conn, this.jobId);

    return mergedDelta;
  }

  private String getAttributesInNewSchema() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < this.latestTable.getAttributes().size(); i++) {
      sb.append(this.latestTable.getAttributes().get(i).name());
      if (i < this.latestTable.getAttributes().size() - 1) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  private HiveManagedTable getNotUpdatedRecords(HiveTable oldTable, HiveTable newTable) throws SQLException {
    LOG.info("Getting records in table " + oldTable.getNameWithJobId(this.jobId) + " but not in table "
        + newTable.getNameWithJobId(this.jobId));

    HiveManagedTable notUpdated =
        new HiveManagedTable.Builder().withName("not_updated").withPrimaryKeys(oldTable.getPrimaryKeys())
            .withAttributes(oldTable.getAttributes()).build();

    notUpdated.createTable(this.conn, this.jobId);
    String leftOuterJoinStmt =
        "INSERT OVERWRITE TABLE " + notUpdated.getNameWithJobId(this.jobId) + " SELECT "
            + oldTable.getNameWithJobId(this.jobId) + ".* FROM " + oldTable.getNameWithJobId(this.jobId)
            + " LEFT OUTER JOIN " + newTable.getNameWithJobId(this.jobId) + " ON "
            + getJoinCondition(oldTable, newTable) + " WHERE " + getKeyIsNullPredicate(newTable);

    this.conn.executeStatements(leftOuterJoinStmt);

    oldTable.dropTable(this.conn, this.jobId);

    return notUpdated;
  }

  private String getJoinCondition(HiveTable firstTable, HiveTable secondTable) {
    if (!firstTable.getPrimaryKeys().equals(secondTable.getPrimaryKeys())) {
      throw new RuntimeException("The primary keys of table " + firstTable.getName() + " and table "
          + secondTable.getName() + " are different");
    }

    boolean addAnd = false;
    StringBuilder sb = new StringBuilder();

    for (String keyAttribute : firstTable.getPrimaryKeys()) {
      if (addAnd) {
        sb.append(" AND ");
      }
      sb.append(firstTable.getNameWithJobId(this.jobId) + "." + keyAttribute + " = "
          + secondTable.getNameWithJobId(this.jobId) + "." + keyAttribute);
      addAnd = true;
    }

    return sb.toString();
  }

  private String getKeyIsNullPredicate(HiveTable table) {
    boolean addAnd = false;
    StringBuilder sb = new StringBuilder();

    for (String keyAttribute : table.getPrimaryKeys()) {
      if (addAnd) {
        sb.append(" AND ");
      }
      sb.append(table.getNameWithJobId(this.jobId) + "." + keyAttribute + " IS NULL");
    }

    return sb.toString();
  }

  private AvroExternalTable unionNotUpdatedRecordsAndDeltas(HiveManagedTable notUpdated, HiveTable mergedDelta)
      throws IOException, SQLException {
    LOG.info("Taking union of table " + notUpdated.getNameWithJobId(this.jobId)
        + "(records in snapshot but not in delta) and table " + mergedDelta.getNameWithJobId(this.jobId)
        + "(merged delta)");

    HiveTable notUpdatedWithNewSchema = notUpdated.addNewColumnsInSchema(this.conn, this.latestTable, this.jobId);
    HiveTable mergedDeltaWithNewSchema = mergedDelta.addNewColumnsInSchema(this.conn, this.latestTable, this.jobId);

    AvroExternalTable outputTable =
        new AvroExternalTable.Builder().withName(this.outputTableName)
            .withPrimaryKeys(this.latestTable.getPrimaryKeys())
            .withSchemaLocation(this.latestTable.getSchemaLocationInHdfs())
            .withDataLocation(this.outputDataLocationInHdfs).build();
    outputTable.createTable(this.conn, this.jobId);

    String unionStmt =
        "INSERT OVERWRITE TABLE " + outputTable.getNameWithJobId(this.jobId) + " SELECT " + getAttributesInNewSchema()
            + " FROM " + notUpdatedWithNewSchema.getNameWithJobId(this.jobId) + " UNION ALL " + "SELECT "
            + getAttributesInNewSchema() + " FROM " + mergedDeltaWithNewSchema.getNameWithJobId(this.jobId);
    this.conn.executeStatements(unionStmt);

    notUpdatedWithNewSchema.dropTable(this.conn, this.jobId);
    mergedDeltaWithNewSchema.dropTable(this.conn, this.jobId);

    return outputTable;
  }

  private void deleteTmpFiles() throws IllegalArgumentException, IOException {
    this.snapshot.deleteTmpFilesIfNeeded();
    for (AvroExternalTable delta : this.deltas) {
      delta.deleteTmpFilesIfNeeded();
    }
  }
}
