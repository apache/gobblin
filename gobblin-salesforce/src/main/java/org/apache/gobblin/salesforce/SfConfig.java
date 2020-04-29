package org.apache.gobblin.salesforce;

import java.util.Properties;
import org.apache.gobblin.typedconfig.Default;
import org.apache.gobblin.typedconfig.Key;
import org.apache.gobblin.typedconfig.QueryBasedSourceConfig;
import org.apache.gobblin.typedconfig.TypedConfig;
import org.apache.gobblin.typedconfig.compiletime.IntRange;


public class SfConfig extends QueryBasedSourceConfig {
  public SfConfig(Properties prop) {
    super(prop);
  }

  @Key("salesforce.partition.pkChunkingSize")@Default("250000")@IntRange({20_000, 250_000})
  public int pkChunkingSize;

  @Key("salesforce.bulkApiUseQueryAll")@Default("false")
  public boolean bulkApiUseQueryAll;

  @Key("salesforce.fetchRetryLimit")@Default("5")
  public int fetchRetryLimit;
}
