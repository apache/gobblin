package gobblin.data.management.partition;

import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyResourcePool;
import gobblin.util.request_allocation.ResourceEstimator;
import gobblin.util.request_allocation.ResourcePool;
import gobblin.util.request_allocation.ResourceRequirement;


/**
 * A {@link ResourceEstimator} that uses a {@link CopyResourcePool} and populates a {@link ResourceRequirement} for a
 * distcp {@link FileSet}.
 */
public class FileSetResourceEstimator implements ResourceEstimator<FileSet<CopyEntity>> {

  static class Factory implements ResourceEstimator.Factory<FileSet<CopyEntity>> {
    @Override
    public ResourceEstimator<FileSet<CopyEntity>> create(Config config) {
      return new FileSetResourceEstimator();
    }
  }

  @Override
  public ResourceRequirement estimateRequirement(FileSet<CopyEntity> copyEntityFileSet, ResourcePool pool) {
    if (!(pool instanceof CopyResourcePool)) {
      throw new IllegalArgumentException("Must use a " + CopyResourcePool.class.getSimpleName());
    }
    CopyResourcePool copyResourcePool = (CopyResourcePool) pool;
    return copyResourcePool.getCopyResourceRequirementBuilder().setEntities(copyEntityFileSet.getTotalEntities())
        .setBytes(copyEntityFileSet.getTotalSizeInBytes()).build();
  }

}
