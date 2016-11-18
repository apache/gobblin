package gobblin.compaction.hivebasedconstructs;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import com.google.common.base.Optional;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.avro.AvroSchemaManager;
import gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.source.extractor.Extractor;
import gobblin.util.AutoReturnableObject;
import gobblin.data.management.conversion.hive.extractor.HiveBaseExtractor;
import lombok.extern.slf4j.Slf4j;


/**
 * {@link Extractor} that extracts primary key field name, delta field name, and location from hive metastore and
 * creates an {@link MRCompactionEntity}
 */
@Slf4j
public class HiveMetadataForCompactionExtractor extends HiveBaseExtractor<Void, MRCompactionEntity> {

  public static final String COMPACTION_PRIMARY_KEY = "hive.metastore.primaryKey";
  public static final String COMPACTION_DELTA = "hive.metastore.delta";

  private MRCompactionEntity compactionEntity;
  private boolean extracted = false;

  public HiveMetadataForCompactionExtractor(WorkUnitState state, FileSystem fs) throws IOException, TException, HiveException {
    if (Boolean.valueOf(state.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY))) {
      log.info("Ignoring Watermark workunit for {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY));
      return;
    }

    HiveWorkUnit hiveWorkUnit = new HiveWorkUnit(state.getWorkunit());
    HiveDataset hiveDataset = hiveWorkUnit.getHiveDataset();
    String dbName = hiveDataset.getDbAndTable().getDb();
    String tableName = hiveDataset.getDbAndTable().getTable();

    HiveMetastoreClientPool pool =
        HiveMetastoreClientPool.get(state.getJobState().getProperties(),
            Optional.fromNullable(state.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
    try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
      Table table = client.get().getTable(dbName, tableName);

      String primaryKey = table.getParameters().get(state.getProp(COMPACTION_PRIMARY_KEY));
      String delta = table.getParameters().get(state.getProp(COMPACTION_DELTA));
      String topicName = AvroSchemaManager.getSchemaFromUrl(hiveWorkUnit.getTableSchemaUrl(), fs).getName();
      String location = new Path(table.getSd().getLocation(), topicName).toString();

      compactionEntity = new MRCompactionEntity(primaryKey, delta, location, state.getProperties());
    }
  }

  @Override
  public MRCompactionEntity readRecord(MRCompactionEntity reuse) {
    if (!extracted) {
      extracted = true;
      return compactionEntity;
    } else {
      return null;
    }
  }

  @Override
  public Void getSchema() throws IOException {
    return null;
  }
}
