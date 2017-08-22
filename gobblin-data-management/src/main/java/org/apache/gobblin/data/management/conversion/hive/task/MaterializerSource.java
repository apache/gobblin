package org.apache.gobblin.data.management.conversion.hive.task;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.data.management.conversion.hive.source.HiveAvroToOrcSource;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.workunit.WorkUnit;


@Slf4j
public class MaterializerSource extends HiveAvroToOrcSource {

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = super.getWorkunits(state);

    for(WorkUnit workUnit : workUnits) {
      if (Boolean.valueOf(workUnit.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY))) {
        log.info("Ignoring Watermark workunit for {}", workUnit.getProp(ConfigurationKeys.DATASET_URN_KEY));
        continue;
      }
      TaskUtils.setTaskFactoryClass(workUnit, HiveTaskFactory.class);
    }
    return workUnits;
  }
}
