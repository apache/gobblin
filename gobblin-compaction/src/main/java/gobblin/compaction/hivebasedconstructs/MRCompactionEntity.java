package gobblin.compaction.hivebasedconstructs;

import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import lombok.Getter;


/**
 * Entity that stores information required for launching an {@link gobblin.compaction.mapreduce.MRCompactor} job
 *
 * {@link #primaryKeyList}: Comma delimited list of fields to use as primary key
 * {@link #deltaList}: Comma delimited list of fields to use as deltaList
 * {@link #dataFilesPath}: Location of files associated with table
 * {@link #props}: Other properties to be passed to {@link gobblin.compaction.mapreduce.MRCompactor}
 */
@Getter
public class MRCompactionEntity {
  private final List<String> primaryKeyList;
  private final List<String> deltaList;
  private final Path dataFilesPath;
  private final Properties props;

  public MRCompactionEntity(List<String> primaryKeyList, List<String> deltaList, Path dataFilesPath, Properties props) {
    this.primaryKeyList = primaryKeyList;
    this.deltaList = deltaList;
    this.dataFilesPath = dataFilesPath;
    this.props = props;
  }
}
