package gobblin.compaction.hivebasedconstructs;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.extractor.HiveBaseExtractor;
import gobblin.data.management.conversion.hive.extractor.HiveBaseExtractorFactory;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;


/**
 * Factory for {@link HiveMetadataForCompactionExtractor}
 */
public class HiveMetadataForCompactionExtractorFactory implements HiveBaseExtractorFactory {
  public HiveBaseExtractor createExtractor(WorkUnitState state, FileSystem sourceFs)
      throws IOException, TException, HiveException {
    return new HiveMetadataForCompactionExtractor(state, sourceFs);
  }
}