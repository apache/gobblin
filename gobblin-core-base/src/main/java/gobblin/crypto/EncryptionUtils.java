package gobblin.crypto;

import gobblin.capability.EncryptionCapabilityParser;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import gobblin.capability.Capability;
import gobblin.writer.StreamEncoder;
import lombok.extern.slf4j.Slf4j;


/**
 * Helper and factory methods for encryption algorithms.
 */
@Slf4j
public class EncryptionUtils {
  private final static Set<String> SUPPORTED_STREAMING_ALGORITHMS =
      ImmutableSet.of("simple", "aes_rotating", EncryptionCapabilityParser.ENCRYPTION_TYPE_ANY);

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
    String encryptionType = EncryptionCapabilityParser.getEncryptionType(parameters);
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
        return new SimpleEncryptor(parameters);
      case EncryptionCapabilityParser.ENCRYPTION_TYPE_ANY:
      case "aes_rotating":
        CredentialStore cs = buildCredentialStore(parameters);
        if (cs == null) {
          throw new IllegalArgumentException("Failed to build credential store; can't instantiate AES");
        }

        return new RotatingAESEncryptor(cs);
      default:
        throw new IllegalArgumentException("Do not support encryption type " + algorithm);
    }
  }

  private static CredentialStore buildCredentialStore(Map<String, Object> parameters) {
    String ks_path = EncryptionCapabilityParser.getKeystorePath(parameters);
    String ks_password = EncryptionCapabilityParser.getKeystorePassword(parameters);

    try {
      return new KeystoreCredentialStore(ks_path, ks_password);
    } catch (IOException e) {
      log.error("Error building credential store, returning null", e);
      return null;
    }
  }

  private EncryptionUtils() {
    // helper functions only, can't instantiate
  }
}
