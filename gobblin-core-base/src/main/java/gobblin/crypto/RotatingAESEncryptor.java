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
 * 4. Ciphertext will be base64 encoded and written out
 */
@Slf4j
public class RotatingAESEncryptor implements StreamEncoder {
  private static final int AES_KEY_LEN = 16;

  private final Random random;
  private final CredentialStore credentialStore;
  private boolean headerWritten;
  private OutputStream origStream;
  private Cipher cipher;
  private String base64Iv;
  private KeyRecord currentKey;
  private List<KeyRecord> keyRecords;

  /**
   * Create a new encryptor
   * @param credentialStore Credential store where keys can be found
   */
  public RotatingAESEncryptor(CredentialStore credentialStore) {
    this.credentialStore = credentialStore;
    this.random = new Random();
    this.headerWritten = false;
  }

  @Override
  public OutputStream wrapOutputStream(OutputStream origStream) {
    this.origStream = origStream;

    rekey();
    final Base64OutputStream base64OutputStream = new Base64OutputStream(origStream);
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

  @Override
  public String getTag() {
    return "aes128_rotating";
  }

  private void rekey() {
    if (origStream == null) {
      throw new IllegalStateException("Can't rekey stream before wrapOutputStream() has been called!");
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

    if (keyRecords.size() == 0) {
      throw new IllegalStateException("Couldn't find any valid keys in store!");
    }

    return keyRecords.get(random.nextInt(keyRecords.size()));
  }

  static class KeyRecord {
    private final int keyId;
    private final SecretKey secretKey;

    public KeyRecord(int keyId, SecretKey secretKey) {
      this.keyId = keyId;
      this.secretKey = secretKey;
    }

    public int getKeyId() {
      return keyId;
    }

    public SecretKey getSecretKey() {
      return secretKey;
    }
  }
}
