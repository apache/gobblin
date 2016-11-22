package gobblin.data.management.conversion.hive.extractor;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import gobblin.configuration.WorkUnitState;


/**
 * Factory interface for {@link HiveBaseExtractor}
 */
public interface HiveBaseExtractorFactory {
  HiveBaseExtractor createExtractor(WorkUnitState state, FileSystem sourceFs)
      throws IOException, TException, HiveException;
}
