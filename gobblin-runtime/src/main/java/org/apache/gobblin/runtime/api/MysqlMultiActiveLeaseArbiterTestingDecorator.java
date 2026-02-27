package org.apache.gobblin.runtime.api;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HostUtils;


/**
 * This class is a decorator for {@link MysqlMultiActiveLeaseArbiter} used to model scenarios where a lease owner fails
 * to complete a lease intermittently (representing a variety of slowness or failure cases that can result on the
 * participant side, network connectivity, or database).
 *
 * It will fail on calls to {@link MysqlMultiActiveLeaseArbiter.recordLeaseSuccess()} where a function of the lease
 * obtained timestamp matches a bitmask of the host. Ideally, each participant should fail on different calls (with
 * limited overlap if we want to test that). We use a deterministic method of failing some calls to complete a lease
 * success with the following methodology. We take the binary representation of the lease obtained timestamp, scatter
 * its bits through bit interleaving of the first and second halves of the binary representation to differentiate
 * behavior of consecutive timestamps, and compare the last N digits (determined through config) to the bit mask of the
 * host. If the bitwise AND comparison to the host bit mask equals the bitmask we fail the call.
 */
@Slf4j
public class MysqlMultiActiveLeaseArbiterTestingDecorator extends MysqlMultiActiveLeaseArbiter {
  private final int bitMaskLength;
  private final int numHosts;
  private final HashMap<Integer, Integer> hostIdToBitMask = new HashMap();

  @Inject
  public MysqlMultiActiveLeaseArbiterTestingDecorator(Config config) throws IOException {
    super(config);
    bitMaskLength = ConfigUtils.getInt(config, ConfigurationKeys.MULTI_ACTIVE_LEASE_ARBITER_BIT_MASK_LENGTH,
        ConfigurationKeys.DEFAULT_MULTI_ACTIVE_LEASE_ARBITER_BIT_MASK_LENGTH);
    numHosts = ConfigUtils.getInt(config, ConfigurationKeys.MULTI_ACTIVE_LEASE_ARBITER_TESTING_DECORATOR_NUM_HOSTS,
        ConfigurationKeys.DEFAULT_MULTI_ACTIVE_LEASE_ARBITER_TESTING_DECORATOR_NUM_HOSTS);
    initializeHostToBitMaskMap(config);
  }

  /**
   * Extract bit mask from input config if one is present. Otherwise set the default bitmask for each host id which
   * does not have overlapping bits between two hosts so that a given status will not fail on multiple hosts.
   * @param config expected to contain a mapping of host address to bitmap in format
   *              "host1:bitMask1,host2:bitMask2,...,hostN:bitMaskN"
   * Note: that if the mapping format is incorrect or there are fewer than `bitMaskLength` mappings provide we utilize
   *               the default to prevent unintended consequences of overlapping bit masks.
   */
  protected void initializeHostToBitMaskMap(Config config) {
    // Set default bit masks for each hosts
    // TODO: change this to parse default from Configuration.Keys property or is that unnecessary?
    hostIdToBitMask.put(1, 0b0001);
    hostIdToBitMask.put(2, 0b0010);
    hostIdToBitMask.put(3, 0b0100);
    hostIdToBitMask.put(4, 0b1000);

    // If a valid mapping is provided in config, then we overwrite all the default values.
    if (config.hasPath(ConfigurationKeys.MULTI_ACTIVE_LEASE_ARBITER_HOST_TO_BIT_MASK_MAP)) {
      String stringMap = config.getString(ConfigurationKeys.MULTI_ACTIVE_LEASE_ARBITER_HOST_TO_BIT_MASK_MAP);
      Optional<HashMap<InetAddress,Integer>> addressToBitMapOptional = validateStringMap(stringMap, numHosts, bitMaskLength);
      if (addressToBitMapOptional.isPresent()) {
        for (InetAddress inetAddress : addressToBitMapOptional.get().keySet()) {
          hostIdToBitMask.put(getHostIdFromAddress(inetAddress), addressToBitMapOptional.get().get(inetAddress));
        }
      }
    }
  }

  protected static Optional<HashMap<InetAddress,Integer>> validateStringMap(String stringMap, int numHosts, int bitMaskLength) {
    // TODO: Refactor to increase abstraction
    String[] hostAddressToMap = stringMap.split(",");
    if (hostAddressToMap.length < numHosts) {
      log.warn("Host address to bit mask map expected to be in format "
          + "`host1:bitMask1,host2:bitMask2,...,hostN:bitMaskN` with at least " + numHosts + " hosts necessary. Using "
          + "default.");
      return Optional.absent();
    }
    HashMap<InetAddress,Integer> addressToBitmap = new HashMap<>();
    for (String mapping : hostAddressToMap) {
      String[] keyAndValue = mapping.split(":");
      if (keyAndValue.length != 2) {
        log.warn("Host address to bit mask map should be separated by `:`. Expected format "
            + "`host1:bitMask1,host2:bitMask2,...,hostN:bitMaskN`. Using default.");
      }
      Optional<InetAddress> addressOptional = HostUtils.getAddressForHostName(keyAndValue[0]);
      if (!addressOptional.isPresent()) {
        log.warn("Invalid hostname format in configuration. Using default.");
        return Optional.absent();
      }
      if (!isValidBitMask(keyAndValue[1], bitMaskLength)) {
        log.warn("Invalid bit mask format in configuration, expected to be " + bitMaskLength + " digit binary number "
            + "ie: `1010`. Using default.");
        return Optional.absent();
      }
      addressToBitmap.put(addressOptional.get(), Integer.valueOf(keyAndValue[1], 2));
    }
    return Optional.of(addressToBitmap);
  }

  protected static boolean isValidBitMask(String input, int bitMaskLength) {
    // Check if the string contains only 0s and 1s
    if (!input.matches("[01]+")) {
      return false;
    }
    // Check if the string is exactly `bitMaskLength` characters long
    if (input.length() != bitMaskLength) {
      return false;
    }
    return true;
  }

  /**
   * Retrieve the host id as a number between 1 through `numHosts` by using the host address's hashcode.
   * @return
   */
  protected int getHostIdFromAddress(InetAddress address) {
      return (address.hashCode() % numHosts) + 1;
  }

  /**
   * Returns bit mask for given host
   * @param hostId
   * @return
   */
  protected int getBitMaskForHostId(int hostId) {
    return this.hostIdToBitMask.get(hostId);
  }

  /**
   * Return bit mask for the current host
   */
  protected int getBitMaskForHost() throws UnknownHostException {
    return getBitMaskForHostId(getHostIdFromAddress(Inet6Address.getLocalHost()));
  }

  /**
   * Apply a deterministic function to the input status to evaluate whether this host should fail to complete a lease
   * for testing purposes.
   */
  @Override
  public boolean recordLeaseSuccess(LeaseObtainedStatus status) throws IOException {
    // Get host bit mask
    int bitMask = getBitMaskForHost();
    if (shouldFailLeaseCompletionAttempt(status, bitMask, bitMaskLength)) {
      log.info("Multi-active lease arbiter lease attempt: [{}, eventTimestamp: {}] - FAILED to complete in testing "
          + "scenario");
      return false;
    } else {
      return super.recordLeaseSuccess(status);
    }
  }

  /**
   * Applies bitmask to lease acquisition timestamp of a status parameter provided to evaluate if the lease attempt to
   * this host should fail
   * @param status {@link org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter.LeaseObtainedStatus}
   * @param bitmask 4-bit binary integer used to compare against modified lease acquisition timestamp
   * @return true if the host should fail the lease completion attempt
   */
  protected static boolean shouldFailLeaseCompletionAttempt(LeaseObtainedStatus status, int bitmask,
      int bitMaskLength) {
    // Convert event timestamp to binary
    Long.toString(status.getLeaseAcquisitionTimestamp()).getBytes();
    String binaryString = Long.toBinaryString(status.getLeaseAcquisitionTimestamp());
    // Scatter binary bits
    String scatteredBinaryString = scatterBinaryStringBits(binaryString);
    // Take last `bitMaskLength`` bits of the string
    int length = scatteredBinaryString.length();
    String shortenedBinaryString = scatteredBinaryString.substring(length-bitMaskLength, length);
    // Apply bitmask
    int binaryInt = Integer.valueOf(shortenedBinaryString, 2);
    return (binaryInt & bitmask) == bitmask;
  }

  /**
   * Given an input string in binary format, scatter the bits to arrange data in a non-contiguous, deterministic way.
   * @param inputBinaryString 64-bit binary string
   * @return a binary format String
   */
  protected static String scatterBinaryStringBits(String inputBinaryString) {
    String firstHalf = inputBinaryString.substring(0, inputBinaryString.length()/2);
    String secondHalf = inputBinaryString.substring(inputBinaryString.length()/2);
    return String.valueOf(interleaveBits(firstHalf, secondHalf));
  }

  /**
   * Interleave bits of two binary strings of the same length to return a binary format long with interleaved bits
   * @param binaryString1 32-bit binary string
   * @param binaryString2 32-bit binary string
   * @return 64-bit binary format long
   */
  protected static long interleaveBits(String binaryString1, String binaryString2) {
    long binaryLong1 = Long.parseLong(binaryString1, 2);
    long binaryLong2 = Long.parseLong(binaryString2, 2);
    long result = 0;
    for (int i=0; i < binaryString1.length(); i++) {
      result |= ((binaryLong1 & (1 << i)) << i) | ((binaryLong2 & (1 << i)) << (i + 1));
    }
    return result;
  }
}