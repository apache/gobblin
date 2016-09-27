package gobblin.data.management.copy.replication;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import lombok.Data;


/**
 * Class used to represent the meta data of the replication
 * @author mitu
 *
 */

@Data
public class ReplicationMetaData {
  private final Optional<Map<String, String>> values;

  public static ReplicationMetaData buildMetaData(Config config) {
    if (!config.hasPath(ReplicationConfiguration.METADATA)) {
      return new ReplicationMetaData(Optional.<Map<String, String>> absent());
    }

    Config metaDataConfig = config.getConfig(ReplicationConfiguration.METADATA);
    Map<String, String> metaDataValues = new HashMap<>();
    Set<Map.Entry<String, ConfigValue>> meataDataEntry = metaDataConfig.entrySet();
    for (Map.Entry<String, ConfigValue> entry : meataDataEntry) {
      metaDataValues.put(entry.getKey(), metaDataConfig.getString(entry.getKey()));
    }

    ReplicationMetaData metaData = new ReplicationMetaData(Optional.of(metaDataValues));
    return metaData;
  }
}
