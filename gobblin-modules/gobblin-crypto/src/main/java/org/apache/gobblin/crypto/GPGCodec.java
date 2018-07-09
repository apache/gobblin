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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.codec.StreamCodec;

/**
 * Codec class that supports GPG encryption and decryption.
 */
public class GPGCodec implements StreamCodec {
  public static final String TAG = "gpg";

  private final String password;
  private final String cipher;
  private final Path keyRingPath;
  private final long keyId;
  private final Configuration conf;

  /**
   * Constructor for a {@code GPGCodec} configured for password-based encryption
   * @param password password for symmetric encryption
   * @param cipher the symmetric cipher to use for encryption. If null or empty then a default cipher is used.
   */
  public GPGCodec(String password, String cipher) {
    this.password = password;
    this.cipher = cipher;

    this.keyRingPath = null;
    this.keyId = 0;
    this.conf = null;
  }

  /**
   * Constructor for a {@code GPGCodec} configured for key-based encryption
   * @param keyRingPath path to the key ring
   * @param passphrase passphrase for retrieving the decryption key
   * @param keyId id for retrieving the key used for encryption
   * @param cipher the symmetric cipher to use for encryption. If null or empty then a default cipher is used.
   */
  public GPGCodec(Path keyRingPath, String passphrase, long keyId, String cipher) {
    this.keyRingPath = keyRingPath;
    this.password = passphrase;
    this.keyId = keyId;
    this.cipher = cipher;
    this.conf = new Configuration();
  }

  @Override
  public OutputStream encodeOutputStream(OutputStream origStream)
      throws IOException {
    if (this.keyRingPath != null) {
      try (InputStream keyRingInputStream = this.keyRingPath.getFileSystem(this.conf).open(this.keyRingPath)) {
        return GPGFileEncryptor.encryptFile(origStream, keyRingInputStream, this.keyId, this.cipher);
      }
    } else {
      return GPGFileEncryptor.encryptFile(origStream, this.password, this.cipher);
    }
  }

  @Override
  public InputStream decodeInputStream(InputStream origStream)
      throws IOException {
    if (this.keyRingPath != null) {
      try (InputStream keyRingInputStream = this.keyRingPath.getFileSystem(this.conf).open(keyRingPath)) {
        return GPGFileDecryptor.decryptFile(origStream, keyRingInputStream, this.password);
      }
    } else {
      return GPGFileDecryptor.decryptFile(origStream, this.password);
    }
  }

  @Override
  public String getTag() {
    return TAG;
  }
}
