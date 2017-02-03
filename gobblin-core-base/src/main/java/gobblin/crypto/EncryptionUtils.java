package gobblin.crypto;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import gobblin.capability.Capability;
import gobblin.writer.StreamEncoder;


/**
 * Helper and factory methods for encryption algorithms.
 */
public class EncryptionUtils {
  private final static Set<String> SUPPORTED_STREAMING_ALGORITHMS =
      ImmutableSet.of("simple", Capability.ENCRYPTION_TYPE_ANY);

  /**
   * Return a set of streaming algorithms (StreamEncoders) that this factory knows how to build
   * @return Set of streaming algorithms the factory knows how to build
   */
  public static Set<String> supportedStreamingAlgorithms() {
    return SUPPORTED_STREAMING_ALGORITHMS;
  }

  /**
   * Return a StreamEncryptor for the given parameters. The algorithm type to use will be extracted
   * from the parameters object.
   * @param parameters Configured parameters for algorithm.
   * @return A StreamEncoder for the requested algorithm
   * @throws IllegalArgumentException If the given algorithm/parameter pair cannot be built
   */
  public static StreamEncoder buildStreamEncryptor(Map<String, Object> parameters) {
    String encryptionType = (String)parameters.get(Capability.ENCRYPTION_TYPE);
    if (encryptionType == null) {
      throw new IllegalArgumentException("Encryption type not present in parameters!");
    }

    return buildStreamEncryptor(encryptionType, parameters);
  }

  /**
   * Return a StreamEncryptor for the given algorithm and with appropriate parameters.
   * @param algorithm ALgorithm to build
   * @param parameters Parameters for algorithm
   * @return A SreamEncoder for that algorithm
   * @throws IllegalArgumentException If the given algorithm/parameter pair cannot be built
   */
  public static StreamEncoder buildStreamEncryptor(String algorithm, Map<String, Object> parameters) {
    /* TODO - Ideally this would dynamically discover plugins somehow which would let us move
     * move crypto algorithms into gobblin-modules and just keep the factory in core. (The factory
     * would fail to build anything if the corresponding gobblin-modules aren't included).
     */
    switch (algorithm) {
      case "simple":
      case Capability.ENCRYPTION_TYPE_ANY:
        return new SimpleEncryptor(parameters);
      default:
        throw new IllegalArgumentException("Do not support encryption type " + algorithm);
    }
  }

  private EncryptionUtils() {
    // helper functions only, can't instantiate
  }
}
