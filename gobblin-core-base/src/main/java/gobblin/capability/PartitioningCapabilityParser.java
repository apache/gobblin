package gobblin.capability;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.partitioner.WriterPartitioner;


/**
 * Extract partitioning-related information from taskState
 */
class PartitioningCapabilityParser extends CapabilityParser {
  PartitioningCapabilityParser() {
    super(ImmutableSet.of(Capability.PARTITIONED_WRITER));
  }

  @Override
  public Collection<CapabilityRecord> parseForBranch(State taskState, int numBranches, int branch) {
    boolean capabilityExists = false;
    Map<String, Object> properties = null;

    if (taskState.contains(ConfigurationKeys.WRITER_PARTITIONER_CLASS)) {
      capabilityExists = true;

      // This does instantiate a partitioner every time we parse config, but assumption is that partitioner
      // construction is cheap.
      String partitionerClass = taskState.getProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS);
      try {
        WriterPartitioner partitioner = WriterPartitioner.class.cast(
            ConstructorUtils.invokeConstructor(Class.forName(partitionerClass), taskState, numBranches, branch));
        properties = ImmutableMap.<String, Object>of(Capability.PARTITIONING_SCHEMA, partitioner.partitionSchema());
      } catch (ReflectiveOperationException e) {
        throw new IllegalArgumentException("Unable to instantiate class " + partitionerClass, e);
      }
    }

    return ImmutableList.of(new CapabilityRecord(Capability.PARTITIONED_WRITER, capabilityExists, properties));
  }
}
