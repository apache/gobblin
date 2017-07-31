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
package org.apache.gobblin.crypto;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.apache.gobblin.codec.Base64Codec;
import org.apache.gobblin.codec.StreamCodec;


/**
 * Implementation of an encryption algorithm that works in the following way:
 *
 * 1. A credentialStore is provisioned with a set of AES keys
 * 2. When encodeOutputStream() is called, an AES key will be picked at random and a new initialization vector (IV)
 *    will be generated.
 * 3. A header will be written [keyId][ivLength][base64 encoded iv]
 * 4. Ciphertext will be base64 encoded and written out. We do not insert linebreaks.
 */
public class RotatingAESCodec implements StreamCodec {
  private static final Logger log = LoggerFactory.getLogger(RotatingAESCodec.class);
  private static final int AES_KEY_LEN = 16;
  private static final String TAG = "aes_rotating";

  private final Random random;
  private final CredentialStore credentialStore;

  /*
   * Cache valid keys in two forms:
   *  A map for retrieving a key quickly (decode case)
   *  An array for quickly selecting a random key (encode case)
   */
  private volatile Map<Integer, KeyRecord> keyRecords_cache;
  private volatile KeyRecord[] keyRecords_cache_arr;

  /**
   * Create a new encryptor
   * @param credentialStore Credential store where keys can be found
   */
  public RotatingAESCodec(CredentialStore credentialStore) {
    this.credentialStore = credentialStore;
    this.random = new Random();
  }

  @Override
  public OutputStream encodeOutputStream(OutputStream origStream)
      throws IOException {
    return new EncodingStreamInstance(selectRandomKey(), origStream).wrapOutputStream();
  }

  @Override
  public InputStream decodeInputStream(InputStream origStream)
      throws IOException {
    return new DecodingStreamInstance(origStream).wrapInputStream();
  }

  private synchronized KeyRecord getKey(Integer key) {
    fillKeyRecords();
    return keyRecords_cache.get(key);
  }

  private synchronized KeyRecord selectRandomKey() {
    KeyRecord[] keyRecords = getKeyRecords();
    if (keyRecords.length == 0) {
      throw new IllegalStateException("Couldn't find any valid keys in store!");
    }

    return keyRecords[random.nextInt(keyRecords.length)];
  }

  private synchronized KeyRecord[] getKeyRecords() {
    fillKeyRecords();
    return keyRecords_cache_arr;
  }

  private synchronized void fillKeyRecords() {
    if (keyRecords_cache == null) {
      keyRecords_cache = new HashMap<>();
      for (Map.Entry<String, byte[]> entry : credentialStore.getAllEncodedKeys().entrySet()) {
        if (entry.getValue().length != AES_KEY_LEN) {
          log.debug("Skipping keyId {} because it is length {}; expected {}", entry.getKey(), entry.getValue().length,
              AES_KEY_LEN);
          continue;
        }

        try {
          Integer keyId = Integer.parseInt(entry.getKey());
          SecretKey key = new SecretKeySpec(entry.getValue(), "AES");

          keyRecords_cache.put(keyId, new KeyRecord(keyId, key));
        } catch (NumberFormatException e) {
          log.debug("Skipping keyId {} because this algorithm can only use numeric key ids", entry.getKey());
        }
      }

      keyRecords_cache_arr = keyRecords_cache.values().toArray(new KeyRecord[keyRecords_cache.size()]);
    }
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
  static class EncodingStreamInstance {
    private final OutputStream origStream;
    private final KeyRecord secretKey;

    private Cipher cipher;
    private String base64Iv;
    private boolean headerWritten = false;

    EncodingStreamInstance(KeyRecord secretKey, OutputStream origStream) {
      this.secretKey = secretKey;
      this.origStream = origStream;
    }

    OutputStream wrapOutputStream()
        throws IOException {
      initCipher();
      final OutputStream base64OutputStream = getBase64Stream(origStream);
      final CipherOutputStream encryptedStream = new CipherOutputStream(base64OutputStream, cipher);

      return new FilterOutputStream(origStream) {
        @Override
        public void write(int b)
            throws IOException {
          writeHeaderIfNecessary();
          encryptedStream.write(b);
        }

        @Override
        public void write(byte[] b)
            throws IOException {
          writeHeaderIfNecessary();
          encryptedStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len)
            throws IOException {
          writeHeaderIfNecessary();
          encryptedStream.write(b, off, len);
        }

        @Override
        public void close()
            throws IOException {
          encryptedStream.close();
        }
      };
    }

    private OutputStream getBase64Stream(OutputStream origStream)
        throws IOException {
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

    private void writeHeaderIfNecessary()
        throws IOException {
      if (!headerWritten) {
        String header = String.format("%04d%03d%s", secretKey.getKeyId(), base64Iv.length(), base64Iv);

        origStream.write(header.getBytes(StandardCharsets.UTF_8));
        this.headerWritten = true;
      }
    }
  }

  private class DecodingStreamInstance {

    private final InputStream origStream;
    private final byte[] buffer = new byte[32];
    private final Cipher cipher;

    DecodingStreamInstance(InputStream origStream)
        throws IOException {
      this.origStream = origStream;

      Integer keyId = readKey();
      KeyRecord key = getKey(keyId);
      if (key == null) {
        throw new IOException("Cannot load key " + String.valueOf(keyId) + " which is specified in input stream");
      }

      try {
        byte[] iv = readIv();
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        if (iv != null) {
          IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
          cipher.init(Cipher.DECRYPT_MODE, key.getSecretKey(), ivParameterSpec);
        } else {
          cipher.init(Cipher.DECRYPT_MODE, key.getSecretKey());
        }
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        throw new IllegalStateException("Failed to load AES which should never happen", e);
      } catch (InvalidKeyException e) {
        throw new IllegalStateException("Failed to parse key from keystore", e);
      } catch (InvalidAlgorithmParameterException e) {
        throw new IllegalStateException("Failed to initialize IV", e);
      }
    }

    InputStream wrapInputStream() throws IOException {
      InputStream base64Decoder = new Base64Codec().decodeInputStream(origStream);
      return new CipherInputStream(base64Decoder, cipher);
    }

    // read and parse key from the bytestream
    private Integer readKey()
        throws IOException {
      IOUtils.readFully(origStream, buffer, 0, 4);
      try {
        return Integer.valueOf(new String(buffer, 0, 4, StandardCharsets.UTF_8));
      } catch (NumberFormatException e) {
        throw new IOException("Expected to be able to parse first 4 bytes of stream as an ASCII keyId");
      }
    }

    private byte[] readIv()
        throws IOException {
      IOUtils.readFully(origStream, buffer, 0, 3);
      Integer ivLen;
      try {
        ivLen = Integer.valueOf(new String(buffer, 0, 3, StandardCharsets.UTF_8));
      } catch (NumberFormatException e) {
        throw new IOException("Expected to parse next 3 bytes of stream as an IV len");
      }

      if (ivLen < 0 || ivLen > buffer.length) {
        throw new IOException(
            "Corrupted data suspected; expected IVLen to be between 0 and " + String.valueOf(buffer.length) + ", read "
                + String.valueOf(ivLen));
      }

      if (ivLen == 0) {
        return null;
      }

      // IV is separately base64 encoded -- none of the standard base64 codec instances support decoding a slice of a
      // byte[] array so create a new buffer here
      byte[] ivBuffer = new byte[ivLen];
      IOUtils.readFully(origStream, ivBuffer, 0, ivBuffer.length);
      return Base64.decodeBase64(ivBuffer);
    }
  }
}
