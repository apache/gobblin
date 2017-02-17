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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import gobblin.type.ContentTypeUtils;
import gobblin.writer.StreamCodec;

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
 * 2. When encodeOutputStream() is called, an AES key will be picked at random and a new initialization vector (IV)
 *    will be generated.
 * 3. A header will be written [keyId][ivLength][base64 encoded iv]
 * 4. Ciphertext will be base64 encoded and written out. We do not insert linebreaks.
 */
@Slf4j
public class RotatingAESCodec implements StreamCodec {
  private static final int AES_KEY_LEN = 16;
  private static final String TAG = "aes_rotating";

  private final Random random;
  private final CredentialStore credentialStore;
  private volatile List<KeyRecord> keyRecords_cache;

  static {
    // this class base64 encodes any potential binary data so it is guaranteed to output utf8
    ContentTypeUtils.getInstance().registerCharsetMapping(TAG, "UTF-8");
  }

  /**
   * Create a new encryptor
   * @param credentialStore Credential store where keys can be found
   */
  public RotatingAESCodec(CredentialStore credentialStore) {
    this.credentialStore = credentialStore;
    this.random = new Random();
  }

  @Override
  public OutputStream encodeOutputStream(OutputStream origStream) throws IOException {
    return new StreamInstance(selectRandomKey(), origStream).wrapOutputStream();
  }

  @Override
  public InputStream decodeInputStream(InputStream origStream) throws IOException {
    throw new UnsupportedOperationException("not implemented yet");
  }

  private synchronized List<KeyRecord> getKeyRecords() {
    if (keyRecords_cache == null) {
      keyRecords_cache = new ArrayList<>();
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

          keyRecords_cache.add(new KeyRecord(keyId, key));
        } catch (NumberFormatException e) {
          log.debug("Skipping keyId {} because this algorithm can only use numeric key ids", entry.getKey());
        }
      }
    }

    return keyRecords_cache;
  }

  private synchronized KeyRecord selectRandomKey() {
    List<KeyRecord> keyRecords = getKeyRecords();
    if (keyRecords.size() == 0) {
      throw new IllegalStateException("Couldn't find any valid keys in store!");
    }

    return keyRecords.get(random.nextInt(keyRecords.size()));
  }

  @Override
  public String getTag() {
    return TAG;
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
    private final KeyRecord secretKey;

    private Cipher cipher;
    private String base64Iv;
    private boolean headerWritten = false;

    StreamInstance(KeyRecord secretKey, OutputStream origStream) {
      this.secretKey = secretKey;
      this.origStream = origStream;
    }

    OutputStream wrapOutputStream() throws IOException {
      initCipher();
      final OutputStream base64OutputStream = getBase64Stream(origStream);
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

    private OutputStream getBase64Stream(OutputStream origStream) throws IOException {
        return new Base64Codec().encodeOutputStream(origStream);
    }

    private void initCipher() {
      if (origStream == null) {
        throw new IllegalStateException("Can't initCipher stream before encodeOutputStream() has been called!");
      }

      try {
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey.getSecretKey());
        byte[] iv = cipher.getIV();
        base64Iv = DatatypeConverter.printBase64Binary(iv);
        this.headerWritten = false;
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        throw new IllegalStateException("Error creating AES algorithm? Should always exist in JRE");
      } catch (InvalidKeyException e) {
        throw new IllegalStateException("Key " + secretKey.getKeyId() + " is illegal - please check credential store");
      }
    }

    private void writeHeaderIfNecessary() throws IOException {
      if (!headerWritten) {
        String header = String.format("%04d%03d%s", secretKey.getKeyId(), base64Iv.length(), base64Iv);

        origStream.write(header.getBytes("UTF-8"));
        this.headerWritten = true;
      }
    }
  }
}
