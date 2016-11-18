package gobblin.compaction.hivebasedconstructs;

import java.util.Properties;
import lombok.Getter;


/**
 * Entity that stores information required for launching an {@link gobblin.compaction.mapreduce.MRCompactor} job
 *
 * {@link #primaryKeyInfo}: Comma delimited list of fields to use as primary key
 * {@link #deltaInfo}: Comma delimited list of fields to use as deltaInfo
 * {@link #location}: Location of files associated with table
 * {@link #props}: Other properties to be passed to {@link gobblin.compaction.mapreduce.MRCompactor}
 */
@Getter
public class MRCompactionEntity {
  private final String primaryKeyInfo;
  private final String deltaInfo;
  private final String location;
  private final Properties props;

  public MRCompactionEntity(String primaryKeyInfo, String deltaInfo, String location, Properties props) {
    this.primaryKeyInfo = primaryKeyInfo;
    this.deltaInfo = deltaInfo;
    this.location = location;
    this.props = props;
  }
}
