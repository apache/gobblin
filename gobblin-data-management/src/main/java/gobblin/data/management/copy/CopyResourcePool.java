package gobblin.data.management.copy;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

import gobblin.util.StringUtils;
import gobblin.util.request_allocation.ResourcePool;
import gobblin.util.request_allocation.ResourceRequirement;

import lombok.Singular;


/**
 * A {@link ResourcePool} for resources used in distcp: total bytes to copy, total number of {@link CopyEntity}s.
 */
public class CopyResourcePool extends ResourcePool {

  public static final String SIZE_KEY = "size";
  public static final String DEFAULT_MAX_SIZE = "10TB";
  public static final String ENTITIES_KEY = "copyEntities";
  public static final int DEFAULT_MAX_ENTITIES = 20000;
  public static final String TOLERANCE_KEY = "boundTolerance";
  public static final double DEFAULT_TOLERANCE = 2.;

  private static final String ENTITIES_DIMENSION = "entities";
  private static final String BYTES_DIMENSION = "bytesCopied";

  /**
   * Parse a {@link CopyResourcePool} from an input {@link Config}.
   */
  public static CopyResourcePool fromConfig(Config limitedScopeConfig) {
    try {
      String sizeStr = limitedScopeConfig.hasPath(SIZE_KEY) ? limitedScopeConfig.getString(SIZE_KEY) : DEFAULT_MAX_SIZE;
      long maxSize = StringUtils.humanReadableToByteCount(sizeStr);
      int maxEntities = limitedScopeConfig.hasPath(ENTITIES_KEY) ? limitedScopeConfig.getInt(ENTITIES_KEY) : DEFAULT_MAX_ENTITIES;
      double tolerance = limitedScopeConfig.hasPath(TOLERANCE_KEY) ? limitedScopeConfig.getDouble(TOLERANCE_KEY) : DEFAULT_TOLERANCE;

      return new CopyResourcePool(ImmutableMap.of(ENTITIES_DIMENSION, (double) maxEntities, BYTES_DIMENSION, (double) maxSize),
          ImmutableMap.of(ENTITIES_DIMENSION, tolerance, BYTES_DIMENSION, tolerance),
          ImmutableMap.<String, Double>of());
    } catch (StringUtils.FormatException fe) {
      throw new RuntimeException(fe);
    }
  }

  private CopyResourcePool(@Singular Map<String, Double> maxResources, @Singular Map<String, Double> tolerances,
      @Singular Map<String, Double> defaults) {
    super(maxResources, tolerances, defaults);
  }

  @Override
  public ResourceRequirement.Builder getResourceRequirementBuilder() {
    return getCopyResourceRequirementBuilder();
  }

  public CopyResourceRequirementBuilder getCopyResourceRequirementBuilder() {
    return new CopyResourceRequirementBuilder(this);
  }

  public class CopyResourceRequirementBuilder extends ResourceRequirement.Builder {
    private CopyResourceRequirementBuilder(CopyResourcePool pool) {
      super(pool);
    }

    /**
     * Set number of {@link CopyEntity}s in {@link gobblin.data.management.partition.FileSet}.
     */
    public CopyResourceRequirementBuilder setEntities(int numberOfEntities) {
      setRequirement(ENTITIES_DIMENSION, (double) numberOfEntities);
      return this;
    }

    /**
     * Set total bytes to copy in {@link gobblin.data.management.partition.FileSet}.
     */
    public CopyResourceRequirementBuilder setBytes(long totalBytes) {
      setRequirement(BYTES_DIMENSION, (double) totalBytes);
      return this;
    }
  }

}
