package gobblin.crypto;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;

import static gobblin.crypto.KeystoreCredentialStore.CreationOptions.*;


@Slf4j
public class KeystoreCredentialStore implements CredentialStore {
  private final KeyStore ks;
  private final char[] password;
  private final Path path;
  private final FileSystem fs;

  enum CreationOptions {
    NONE, CREATE_IF_MISSING
  };

  public KeystoreCredentialStore(String path, String passwordStr) throws IOException {
    this(path, passwordStr, EnumSet.noneOf(CreationOptions.class));
  }

  public KeystoreCredentialStore(String path, String passwordStr, EnumSet<CreationOptions> options) throws IOException {
    this(FileSystem.get(new Configuration()), new Path(path), passwordStr, options);
  }

  public KeystoreCredentialStore(FileSystem fs, Path path, String passwordStr, EnumSet<CreationOptions> options) throws IOException {
    try {
      this.ks = KeyStore.getInstance("JCEKS");
      this.password = passwordStr.toCharArray();
      this.path = path;
      this.fs = fs;

      if (!fs.exists(path)) {
        if (options.contains(CREATE_IF_MISSING)) {
          log.info("No keystore found at " + path + ", creating from scratch");
          ks.load(null, password);
        } else {
          throw new IllegalArgumentException("Keystore " + path + " does not exist");
        }
      } else {
        try (InputStream fis = fs.open(path)) {
          ks.load(fis, password);
          log.info("Successfully loaded keystore from " + path);
        }
      }
    } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Unexpected failure initializing keystore", e);
    }
  }

  @Override
  public byte[] getEncodedKey(String id) {
    try {
      Key k = ks.getKey(id, password);
      return (k == null) ? null : k.getEncoded();
    } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
      // log
      return null;
    }
  }

  @Override
  public Map<String, byte[]> getAllEncodedKeys() {
    Map<String, byte[]> ret = new HashMap<>();
    try {
      Enumeration<String> aliases = ks.aliases();
      try {
        while (aliases.hasMoreElements()) {
          String key = aliases.nextElement();
          if (ks.isKeyEntry(key)) {
            ret.put(key, getEncodedKey(key));
          }
        }
      } catch (KeyStoreException e) {
        // log, continue loop
      }
    } catch (KeyStoreException e) {
      // TODO lpg
      return ret;
    }

    return ret;
  }

  public void generateAesKeys(int numKeys) throws IOException, KeyStoreException {
    for (int i = 1; i <= numKeys; i++) {
      SecretKey key = generateKey();
      ks.setEntry(String.valueOf(i), new KeyStore.SecretKeyEntry(key), new KeyStore.PasswordProtection(password));
    }

    saveKeystore();
  }

  private SecretKey generateKey() {
    SecureRandom r = new SecureRandom();
    byte[] keyBytes = new byte[16];
    r.nextBytes(keyBytes);

    return new SecretKeySpec(keyBytes, "AES");
  }

  private void saveKeystore() throws IOException {
    try (OutputStream fOs = fs.create(path, true)) {
      ks.store(fOs, password);
    } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
      throw new IOException("Error serializing keystore", e);
    }
  }
}
