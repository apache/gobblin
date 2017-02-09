package gobblin.capability;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;


/**
 * Extract encryption related information from taskState
 */
class EncryptionCapabilityParser extends CapabilityParser {
  EncryptionCapabilityParser() {
    super(ImmutableSet.of(Capability.ENCRYPTION));
  }

  @Override
  public Collection<CapabilityRecord> parseForBranch(State taskState, int numBranches, int branch) {
    boolean capabilityExists = false;
    Map<String, Object> properties = new HashMap<>();

    String propName =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_ENABLE_ENCRYPT, numBranches, branch);

    String type = taskState.getProp(propName);
    if (type != null) {
      type = type.toLowerCase();

      if (type.equals("true")) {
        type = Capability.ENCRYPTION_TYPE_ANY;
      }

      if (!type.equals("false")) {
        capabilityExists = true;
        properties.put(Capability.ENCRYPTION_TYPE, type);
      }
    }

    String ksPathPropName =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_ENCRYPT_KEYSTORE_LOCATION, numBranches,
            branch);
    String ksPath = taskState.getProp(ksPathPropName);

    if (ksPath != null) {
      properties.put("ks_path", ksPath);
    }
    return ImmutableList.of(new CapabilityRecord(Capability.ENCRYPTION, capabilityExists, properties));
  }
}
