/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.crypto;

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


/**
 * A CredentialStore that uses Java's Keystore implementation to store and retrieve keys.
 */
@Slf4j
public class JCEKSKeystoreCredentialStore implements CredentialStore {
  private final KeyStore ks;
  private final char[] password;
  private final Path path;
  private final FileSystem fs;

  /**
   * Options that can be used while instantiating a new keystore
   */
  enum CreationOptions {
    /**
     * Create an empty keystore if one can't be found. Otherwise an exception will be thrown.
     */
    CREATE_IF_MISSING
  }

  /**
   * Instantiate a new keystore at the given path protected by a password.
   * @param path Path to find the keystore
   * @param passwordStr Password the keystore is protected with
   * @throws IOException If the keystore cannot be loaded because the password is wrong or the file has been corrupted.
   * @throws IllegalArgumentException If the keystore does not exist at the given path.
   */
  public JCEKSKeystoreCredentialStore(String path, String passwordStr) throws IOException {
    this(path, passwordStr, EnumSet.noneOf(CreationOptions.class));
  }

  /**
   * Instantiate a new keystore at the given path protected by a password.
   * @param path Path to find the keystore
   * @param passwordStr Password the keystore is protected with
   * @param options Flags for keystore creation
   * @throws IOException If the keystore cannot be loaded because of a corrupt file or the password is wrong
   * @throws IllegalArgumentException If CREATE_IF_MISSING is not present in options and the keystore does not exist
   * at the given path.
   */
  public JCEKSKeystoreCredentialStore(String path, String passwordStr, EnumSet<CreationOptions> options) throws IOException {
    this(new Path(path), passwordStr, options);
  }

  /**
   * Instantiate a new keystore at the given path protected by a password.
   * @param path Path to find the keystore
   * @param passwordStr Password the keystore is protected with
   * @param options Flags for keystore creation
   * @throws IOException If the keystore cannot be loaded because of a corrupt file or the password is wrong
   * @throws IllegalArgumentException If CREATE_IF_MISSING is not present in options and the keystore does not exist
   * at the given path.
   */
  public JCEKSKeystoreCredentialStore(Path path, String passwordStr, EnumSet<CreationOptions> options) throws IOException {
    try {
      this.ks = KeyStore.getInstance("JCEKS");
      this.password = passwordStr.toCharArray();
      this.path = path;
      this.fs = path.getFileSystem(new Configuration());

      if (!fs.exists(path)) {
        if (options.contains(CreationOptions.CREATE_IF_MISSING)) {
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
      log.warn("Error trying to decode key " + id, e);
      return null;
    }
  }

  @Override
  public Map<String, byte[]> getAllEncodedKeys() {
    Map<String, byte[]> ret = new HashMap<>();
    try {
      Enumeration<String> aliases = ks.aliases();
      while (aliases.hasMoreElements()) {
        String key = aliases.nextElement();
        try {
          if (ks.isKeyEntry(key)) {
            ret.put(key, getEncodedKey(key));
          }
        } catch (KeyStoreException e) {
          log.warn("Error trying to decode key id " + key + ", not returning in list", e);
        }
      }
    } catch (KeyStoreException e) {
      log.warn("Error retrieving all aliases in keystore; treating as empty", e);
      return ret;
    }

    return ret;
  }

  /**
   * Generate a set of AES keys for the store. The key ids will simple be (startOffset ... startOffset + numKeys).
   * @param numKeys Number of keys to generate
   * @param startOffset ID to start generating keys with
   * @throws IOException If there is an error serializing the keystore back to disk
   * @throws KeyStoreException If there is an error serializing the keystore back to disk
   */
  public void generateAesKeys(int numKeys, int startOffset) throws IOException, KeyStoreException {
    for (int i = 1; i <= numKeys; i++) {
      SecretKey key = generateKey();
      ks.setEntry(String.valueOf(i + startOffset), new KeyStore.SecretKeyEntry(key),
          new KeyStore.PasswordProtection(password));
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
