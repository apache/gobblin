package gobblin.compaction.hivebasedconstructs;

import java.util.Properties;
import lombok.Getter;


/**
 * Entity that stores information required for launching an {@link gobblin.compaction.mapreduce.MRCompactor} job
 */
@Getter
public class MRCompactionEntity {
  private final String primaryKey;
  private final String delta;
  private final String location;
  private final Properties props;

  public MRCompactionEntity(String primaryKey, String delta, String location, Properties props) {
    this.primaryKey = primaryKey;
    this.delta = delta;
    this.location = location;
    this.props = props;
  }
}
