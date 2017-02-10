package gobblin.crypto;

import java.util.Map;


/**
 * Interface for a simple CredentialStore that simply has a set of byte-encoded keys. Format
 * of the underlying keys is left to the implementor.
 */
public interface CredentialStore {
  byte[] getEncodedKey(String id);

  Map<String, byte[]> getAllEncodedKeys();
}
