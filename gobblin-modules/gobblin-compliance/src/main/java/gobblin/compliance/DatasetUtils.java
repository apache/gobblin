package gobblin.compliance;

import java.util.List;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;


/**
 * Created by adsharma on 3/28/17.
 */
@Slf4j
public class DatasetUtils {
  public static Optional<HivePartitionDataset> findDataset(String partitionName, List<HivePartitionDataset> datasets) {
    for (HivePartitionDataset dataset : datasets) {
      if (dataset.datasetURN().equalsIgnoreCase(partitionName)) {
        return Optional.fromNullable(dataset);
      }
    }
    log.warn("Unable to find dataset corresponding to " + partitionName);
    return Optional.<HivePartitionDataset>absent();
  }

  public static String getPropertyFromDataset(HivePartitionDataset dataset, String property, int defaultValue) {
    Optional<String> propertyValueOptional = Optional.fromNullable(dataset.getParams().get(property));
    if (!propertyValueOptional.isPresent()) {
      return Integer.toString(defaultValue);
    }
    try {
      int propertyVal = Integer.parseInt(propertyValueOptional.get());
      if (propertyVal < 0) {
        return Integer.toString(defaultValue);
      } else {
        return Integer.toString(propertyVal);
      }
    } catch (NumberFormatException e) {
      return Integer.toString(defaultValue);
    }
  }
}
