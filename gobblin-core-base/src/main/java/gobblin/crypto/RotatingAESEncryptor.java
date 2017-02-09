package gobblin.crypto;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.codec.binary.Base64OutputStream;

import gobblin.writer.StreamEncoder;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import lombok.extern.slf4j.Slf4j;


/**
 * Implementation of an encryption algorithm that works in the following way:
 *
 * 1. A credentialStore is provisioned with a set of AES keys
 * 2. When wrapOutputStream() is called, an AES key will be picked at random and a new initialization vector (IV)
 *    will be generated.
 * 3. A header will be written [keyId][ivLength][base64 encoded iv]
 * 4. Ciphertext will be base64 encoded and written out. We do not insert linebreaks.
 */
@Slf4j
public class RotatingAESEncryptor implements StreamEncoder {
  private static final int AES_KEY_LEN = 16;

  private final Random random;
  private final CredentialStore credentialStore;
  private List<KeyRecord> keyRecords;

  /**
   * Create a new encryptor
   * @param credentialStore Credential store where keys can be found
   */
  public RotatingAESEncryptor(CredentialStore credentialStore) {
    this.credentialStore = credentialStore;
    this.random = new Random();
  }

  @Override
  public OutputStream wrapOutputStream(OutputStream origStream) {
    return new StreamInstance(random, getKeyRecords(), origStream).wrapOutputStream();


  }

  private synchronized List<KeyRecord> getKeyRecords() {
    if (keyRecords == null) {
      keyRecords = new ArrayList<>();
      for (Map.Entry<String, byte[]> entry : credentialStore.getAllEncodedKeys().entrySet()) {
        if (entry.getValue().length != AES_KEY_LEN) {
          log.debug("Skipping keyId {} because it is length {}; expected {}",
              entry.getKey(),
              entry.getValue().length,
              AES_KEY_LEN);
          continue;
        }

        try {
          Integer keyId = Integer.parseInt(entry.getKey());
          SecretKey key = new SecretKeySpec(entry.getValue(), "AES");

          keyRecords.add(new KeyRecord(keyId, key));
        } catch (NumberFormatException e) {
          log.debug("Skipping keyId {} because this algorithm can only use numeric key ids", entry.getKey());
        }
      }
    }

    return keyRecords;
  }

  @Override
  public String getTag() {
    return "aes128_rotating";
  }

  /**
   * Represents a set of parsed AES keys that we can choose from when encrypting.
   */
  static class KeyRecord {
    private final int keyId;
    private final SecretKey secretKey;

    KeyRecord(int keyId, SecretKey secretKey) {
      this.keyId = keyId;
      this.secretKey = secretKey;
    }

    int getKeyId() {
      return keyId;
    }

    SecretKey getSecretKey() {
      return secretKey;
    }
  }

  /**
   * Helper class that keeps state around for a wrapped output stream. Each stream will have a different
   * selected key, IV, and cipher state.
   */
  static class StreamInstance {
    private final OutputStream origStream;
    private final List<KeyRecord> keyRecords;
    private final Random random;

    private Cipher cipher;
    private String base64Iv;
    private KeyRecord currentKey;
    private boolean headerWritten = false;


    StreamInstance(Random random, List<KeyRecord> keyRecords, OutputStream origStream) {
      this.random = random;
      this.origStream = origStream;
      this.keyRecords = keyRecords;
    }

    OutputStream wrapOutputStream() {
      initCipher();
      final Base64OutputStream base64OutputStream = new Base64OutputStream(origStream, true, 0, null);
      final CipherOutputStream encryptedStream = new CipherOutputStream(base64OutputStream, cipher);

      return new FilterOutputStream(origStream) {
        @Override
        public void write(int b) throws IOException {
          writeHeaderIfNecessary();
          encryptedStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
          writeHeaderIfNecessary();
          encryptedStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          writeHeaderIfNecessary();
          encryptedStream.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
          encryptedStream.close();
        }
      };
    }

    private void initCipher() {
      if (origStream == null) {
        throw new IllegalStateException("Can't initCipher stream before wrapOutputStream() has been called!");
      }

      try {
        currentKey = getNewKey();

        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, currentKey.getSecretKey());
        byte[] iv = cipher.getIV();
        base64Iv = DatatypeConverter.printBase64Binary(iv);
        this.headerWritten = false;
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        throw new IllegalStateException("Error creating AES algorithm? Should always exist in JRE");
      } catch (InvalidKeyException e) {
        throw new IllegalStateException(
            "Key id " + String.valueOf(currentKey.getKeyId()) + " is illegal - please check credential store");
      }
    }

    private void writeHeaderIfNecessary() throws IOException {
      if (!headerWritten) {
        String header = String.format("%04d%03d%s", currentKey.getKeyId(), base64Iv.length(), base64Iv);

        origStream.write(header.getBytes("UTF-8"));
        this.headerWritten = true;
      }
    }

    private KeyRecord getNewKey() {
      if (keyRecords.size() == 0) {
        throw new IllegalStateException("Couldn't find any valid keys in store!");
      }

      return keyRecords.get(random.nextInt(keyRecords.size()));
    }
  }
}
