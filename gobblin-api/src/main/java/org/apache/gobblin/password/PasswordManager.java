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

package org.apache.gobblin.password;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jasypt.util.text.BasicTextEncryptor;
import org.jasypt.util.text.StrongTextEncryptor;
import org.jasypt.util.text.TextEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Closer;
import com.google.common.io.LineReader;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

/**
 * A class for managing password encryption and decryption. To encrypt or decrypt a password, a master password
 * should be provided which is used as encryption or decryption key.
 * Encryption is done with the single key provided.
 * Decryption is tried with multiple keys to facilitate key rotation.
 * If the master key file provided is /var/tmp/masterKey.txt, decryption is tried with keys at
 * /var/tmp/masterKey.txt, /var/tmp/masterKey.txt.1, /var/tmp/masterKey.txt.2, and so on and so forth till
 * either any such file does not exist or {@code this.NUMBER_OF_ENCRYPT_KEYS} attempts have been made.
 *
 * @author Ziyang Liu
 */
public class PasswordManager {

  private static final Logger LOG = LoggerFactory.getLogger(PasswordManager.class);

  private static final long CACHE_SIZE = 100;
  private static final long CACHE_EXPIRATION_MIN = 10;
  private static final Pattern PASSWORD_PATTERN = Pattern.compile("ENC\\((.*)\\)");
  private final boolean useStrongEncryptor;
  private FileSystem fs;
  private List<TextEncryptor> encryptors;

  private static final LoadingCache<CachedInstanceKey, PasswordManager> CACHED_INSTANCES =
      CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).expireAfterAccess(CACHE_EXPIRATION_MIN, TimeUnit.MINUTES)
          .build(new CacheLoader<CachedInstanceKey, PasswordManager>() {
            @Override
            public PasswordManager load(CachedInstanceKey cacheKey) {
              return new PasswordManager(cacheKey);
            }
          });

  private PasswordManager(CachedInstanceKey cacheKey) {
    this.useStrongEncryptor = cacheKey.useStrongEncryptor;

    try {
      this.fs = cacheKey.fsURI != null ? FileSystem.get(URI.create(cacheKey.fsURI), new Configuration())
          : (cacheKey.masterPasswordFile != null ? new Path(cacheKey.masterPasswordFile).getFileSystem(new Configuration()) : null);
    } catch (IOException e) {
      LOG.warn("Failed to instantiate FileSystem.", e);
    }
    this.encryptors = getEncryptors(cacheKey);
  }

  private List<TextEncryptor> getEncryptors(CachedInstanceKey cacheKey) {
    List<TextEncryptor> encryptors = new ArrayList<>();
    int numOfEncryptionKeys = cacheKey.numOfEncryptionKeys;
    String suffix = "";
    int i = 1;

    if (cacheKey.masterPasswordFile == null || numOfEncryptionKeys < 1) {
      return encryptors;
    }

    Exception exception = null;

    do {
      Path currentMasterPasswordFile = new Path(cacheKey.masterPasswordFile + suffix);
      try (Closer closer = Closer.create()) {
        if (!fs.exists(currentMasterPasswordFile) ||
            fs.getFileStatus(currentMasterPasswordFile).isDirectory()) {
          continue;
        }
        InputStream in = closer.register(fs.open(currentMasterPasswordFile));
        String masterPassword = new LineReader(new InputStreamReader(in, Charsets.UTF_8)).readLine();
        TextEncryptor encryptor = useStrongEncryptor ? new StrongTextEncryptor() : new BasicTextEncryptor();
        // setPassword() needs to be called via reflection since the TextEncryptor interface doesn't have this method.
        encryptor.getClass().getMethod("setPassword", String.class).invoke(encryptor, masterPassword);
        encryptors.add(encryptor);
        suffix = "." + String.valueOf(i);
      } catch (FileNotFoundException fnf) {
        // It is ok for password files not being present
        LOG.warn("Master password file " + currentMasterPasswordFile + " not found.");
      } catch (IOException ioe) {
        exception = ioe;
        LOG.warn("Master password could not be read from file " + currentMasterPasswordFile);
      } catch (Exception e) {
        LOG.warn("Encryptor could not be instantiated.");
      }
    } while (i++ < numOfEncryptionKeys);

    // Throw exception if could not read any existing password file
    if (encryptors.size() < 1 && exception != null) {
      throw new RuntimeException("Master Password could not be read from any master password file.", exception);
    }
    return encryptors;
  }

  /**
   * Get an instance with no master password, which cannot encrypt or decrypt passwords.
   */
  public static PasswordManager getInstance() {
    try {
      return CACHED_INSTANCES.get(new CachedInstanceKey());
    } catch (ExecutionException e) {
      throw new RuntimeException("Unable to get an instance of PasswordManager", e);
    }
  }

  /**
   * Get an instance. The location of the master password file is provided via "encrypt.key.loc".
   */
  public static PasswordManager getInstance(State state) {
    try {
      return CACHED_INSTANCES
          .get(new CachedInstanceKey(state));
    } catch (ExecutionException e) {
      throw new RuntimeException("Unable to get an instance of PasswordManager", e);
    }
  }

  /**
   * Get an instance. The location of the master password file is provided via "encrypt.key.loc".
   */
  public static PasswordManager getInstance(Properties props) {
    return getInstance(new State(props));
  }

  /**
   * Get an instance. The master password file is given by masterPwdLoc.
   */
  public static PasswordManager getInstance(Path masterPwdLoc) {
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdLoc.toString());
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_FS_URI, masterPwdLoc.toUri());
    try {
      return CACHED_INSTANCES
          .get(new CachedInstanceKey(state));
    } catch (ExecutionException e) {
      throw new RuntimeException("Unable to get an instance of PasswordManager", e);
    }
  }

  private static boolean shouldUseStrongEncryptor(State state) {
    return state.getPropAsBoolean(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR,
        ConfigurationKeys.DEFAULT_ENCRYPT_USE_STRONG_ENCRYPTOR);
  }

  /**
   * Encrypt a password. A master password must have been provided in the constructor.
   * @param plain A plain password to be encrypted.
   * @return The encrypted password.
   */
  public String encryptPassword(String plain) {
    Preconditions.checkArgument(this.encryptors.size() > 0,
        "A master password needs to be provided for encrypting passwords.");

    try {
      return this.encryptors.get(0).encrypt(plain);
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt password", e);
    }
  }

  /**
   * Decrypt an encrypted password. A master password file must have been provided in the constructor.
   * @param encrypted An encrypted password.
   * @return The decrypted password.
   */
  public String decryptPassword(String encrypted) {
    Preconditions.checkArgument(this.encryptors.size() > 0,
        "A master password needs to be provided for decrypting passwords.");

    for (TextEncryptor encryptor : encryptors) {
      try {
        return encryptor.decrypt(encrypted);
      } catch (Exception e) {
        LOG.warn("Failed attempt to decrypt secret {}", encrypted, e);
      }
    }
    LOG.error("All {} decrypt attempt(s) failed.", encryptors.size());
    throw new RuntimeException("Failed to decrypt password ENC(" + encrypted + ")");
  }

  /**
   * Decrypt a password if it is an encrypted password (in the form of ENC(.*))
   * and a master password file has been provided in the constructor.
   * Otherwise, return the password as is.
   */
  public String readPassword(String password) {
    if (password == null || encryptors.size() < 1) {
      return password;
    }
    Matcher matcher = PASSWORD_PATTERN.matcher(password);
    if (matcher.find()) {
      return this.decryptPassword(matcher.group(1));
    }
    return password;
  }

  public static Optional<String> getMasterPassword(Path masterPasswordFile) {
    try {
      FileSystem fs = masterPasswordFile.getFileSystem(new Configuration());
      return getMasterPassword(fs, masterPasswordFile);
    } catch (IOException e) {
      throw new RuntimeException("Failed to obtain master password from " + masterPasswordFile, e);
    }
  }

  public static Optional<String> getMasterPassword(FileSystem fs, Path masterPasswordFile) {
    try (Closer closer = Closer.create()) {
      if (!fs.exists(masterPasswordFile) || fs.getFileStatus(masterPasswordFile).isDirectory()) {
        LOG.warn(masterPasswordFile + " does not exist or is not a file. Cannot decrypt any encrypted password.");
        return Optional.absent();
      }
      InputStream in = closer.register(fs.open(masterPasswordFile));
      return Optional.of(new LineReader(new InputStreamReader(in, Charsets.UTF_8)).readLine());
    } catch (IOException e) {
      throw new RuntimeException("Failed to obtain master password from " + masterPasswordFile, e);
    }
  }

  @EqualsAndHashCode
  private static class CachedInstanceKey {
    int numOfEncryptionKeys;
    String fsURI;
    String masterPasswordFile;
    boolean useStrongEncryptor;

    public CachedInstanceKey(State state) {
      this.numOfEncryptionKeys = state.getPropAsInt(ConfigurationKeys.NUMBER_OF_ENCRYPT_KEYS, ConfigurationKeys.DEFAULT_NUMBER_OF_MASTER_PASSWORDS);
      this.useStrongEncryptor = shouldUseStrongEncryptor(state);
      this.fsURI = state.getProp(ConfigurationKeys.ENCRYPT_KEY_FS_URI);
      this.masterPasswordFile = state.getProp(ConfigurationKeys.ENCRYPT_KEY_LOC);
    }

    public CachedInstanceKey() {

    }
  }
}