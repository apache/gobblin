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

package gobblin.security.ssl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

import org.apache.commons.io.FileUtils;

import com.typesafe.config.Config;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import gobblin.password.PasswordManager;
import gobblin.util.ConfigUtils;


/**
 * Provide different approaches to create a {@link SSLContext}
 */
public class SSLContextFactory {
  public static final String KEY_STORE_FILE_PATH = "keyStoreFilePath";
  public static final String KEY_STORE_PASSWORD = "keyStorePassword";
  public static final String KEY_STORE_TYPE = "keyStoreType";
  public static final String TRUST_STORE_FILE_PATH = "trustStoreFilePath";
  public static final String TRUST_STORE_PASSWORD = "trustStorePassword";

  private static final String DEFAULT_ALGORITHM = "SunX509";
  private static final String DEFAULT_PROTOCOL = "TLS";
  private static final String JKS_STORE_TYPE_NAME = "JKS";
  private static final String P12_STORE_TYPE_NAME = "PKCS12";

  /**
   * Create a {@link SSLContext} instance
   *
   * @param keyStoreFile a p12 or jks file depending on key store type
   * @param keyStorePassword password to access the key store
   * @param keyStoreType type of key store
   * @param trustStoreFile a jks file
   * @param trustStorePassword password to access the trust store
   */
  public static SSLContext createInstance(File keyStoreFile, String keyStorePassword, String keyStoreType, File trustStoreFile,
      String trustStorePassword) {
    if (!keyStoreType.equalsIgnoreCase(P12_STORE_TYPE_NAME) && !keyStoreType.equalsIgnoreCase(JKS_STORE_TYPE_NAME)) {
      throw new IllegalArgumentException("Unsupported keyStoreType: " + keyStoreType);
    }

    try {
      // Load KeyStore
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(toInputStream(keyStoreFile), keyStorePassword.toCharArray());

      // Load TrustStore
      KeyStore trustStore = KeyStore.getInstance(JKS_STORE_TYPE_NAME);
      trustStore.load(toInputStream(trustStoreFile), trustStorePassword.toCharArray());

      // Set KeyManger from keyStore
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(DEFAULT_ALGORITHM);
      kmf.init(keyStore, keyStorePassword.toCharArray());

      // Set TrustManager from trustStore
      TrustManagerFactory trustFact = TrustManagerFactory.getInstance(DEFAULT_ALGORITHM);
      trustFact.init(trustStore);

      // Set Context to TLS and initialize it
      SSLContext sslContext = SSLContext.getInstance(DEFAULT_PROTOCOL);
      sslContext.init(kmf.getKeyManagers(), trustFact.getTrustManagers(), null);

      return sslContext;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a {@link SSLContext} from a {@link Config}
   *
   * <p>
   *   A sample configuration is:
   *   <br> keyStoreFilePath=/path/to/key/store
   *   <br> keyStorePassword=password
   *   <br> keyStoreType=PKCS12
   *   <br> trustStoreFilePath=/path/to/trust/store
   *   <br> trustStorePassword=password
   * </p>
   *
   * @param srcConfig configuration
   * @return an instance of {@link SSLContext}
   */
  public static SSLContext createInstance(Config srcConfig) {
    // srcConfig.getString() will throw ConfigException if any key is missing
    String keyStoreFilePath = srcConfig.getString(KEY_STORE_FILE_PATH);
    String trustStoreFilePath = srcConfig.getString(TRUST_STORE_FILE_PATH);
    PasswordManager passwdMgr = PasswordManager.getInstance(ConfigUtils.configToState(srcConfig));
    String keyStorePassword = passwdMgr.readPassword(srcConfig.getString(KEY_STORE_PASSWORD));
    String trustStorePassword = passwdMgr.readPassword(srcConfig.getString(TRUST_STORE_PASSWORD));

    return createInstance(new File(keyStoreFilePath), keyStorePassword, srcConfig.getString(KEY_STORE_TYPE),
        new File(trustStoreFilePath), trustStorePassword);
  }

  private static InputStream toInputStream(File storeFile)
      throws IOException {
    byte[] data = FileUtils.readFileToByteArray(storeFile);
    return new ByteArrayInputStream(data);
  }
}

