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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


/**
 * A class for managing password encryption and decryption. To encrypt or decrypt a password, a master password
 * should be provided which is used as encryption or decryption key.
 *
 * @author Ziyang Liu
 */
public class PasswordManager {

  private static final Logger LOG = LoggerFactory.getLogger(PasswordManager.class);

  private static final long CACHE_SIZE = 100;
  private static final long CACHE_EXPIRATION_MIN = 10;
  private static final Pattern PASSWORD_PATTERN = Pattern.compile("ENC\\((.*)\\)");

  private static final LoadingCache<Map.Entry<Optional<String>, Boolean>, PasswordManager> CACHED_INSTANCES =
      CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).expireAfterAccess(CACHE_EXPIRATION_MIN, TimeUnit.MINUTES)
          .build(new CacheLoader<Map.Entry<Optional<String>, Boolean>, PasswordManager>() {

            @Override
            public PasswordManager load(Map.Entry<Optional<String>, Boolean> cacheKey) {
              return new PasswordManager(cacheKey.getKey(), cacheKey.getValue());
            }
          });

  private Optional<TextEncryptor> encryptor;

  private PasswordManager(Optional<String> masterPassword, boolean useStrongEncryptor) {
    if (masterPassword.isPresent()) {
      this.encryptor = useStrongEncryptor ? Optional.of((TextEncryptor) new StrongTextEncryptor())
          : Optional.of((TextEncryptor) new BasicTextEncryptor());
      try {

        // setPassword() needs to be called via reflection since the TextEncryptor interface doesn't have this method.
        this.encryptor.get().getClass().getMethod("setPassword", String.class).invoke(this.encryptor.get(),
            masterPassword.get());
      } catch (Exception e) {
        LOG.error("Failed to set master password for encryptor", e);
        this.encryptor = Optional.absent();
      }
    } else {
      this.encryptor = Optional.absent();
    }
  }

  /**
   * Get an instance with no master password, which cannot encrypt or decrypt passwords.
   */
  public static PasswordManager getInstance() {
    try {
      Optional<String> absent = Optional.absent();
      return CACHED_INSTANCES.get(new AbstractMap.SimpleEntry<>(absent, shouldUseStrongEncryptor(new State())));
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
          .get(new AbstractMap.SimpleEntry<>(getMasterPassword(state), shouldUseStrongEncryptor(state)));
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
    try {
      return CACHED_INSTANCES
          .get(new AbstractMap.SimpleEntry<>(getMasterPassword(masterPwdLoc), shouldUseStrongEncryptor(new State())));
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
    Preconditions.checkArgument(this.encryptor.isPresent(),
        "A master password needs to be provided for encrypting passwords.");

    try {
      return this.encryptor.get().encrypt(plain);
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt password", e);
    }
  }

  /**
   * Decrypt an encrypted password. A master password must have been provided in the constructor.
   * @param encrypted An encrypted password.
   * @return The decrypted password.
   */
  public String decryptPassword(String encrypted) {
    Preconditions.checkArgument(this.encryptor.isPresent(),
        "A master password needs to be provided for decrypting passwords.");

    try {
      return this.encryptor.get().decrypt(encrypted);
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt password " + encrypted, e);
    }
  }

  /**
   * Decrypt a password if it is an encrypted password (in the form of ENC(.*)), and a master password has been
   * provided in the constructor. Otherwise, return the password as is.
   */
  public String readPassword(String password) {
    if (password == null || !this.encryptor.isPresent()) {
      return password;
    }
    Matcher matcher = PASSWORD_PATTERN.matcher(password);
    if (matcher.find()) {
      return this.decryptPassword(matcher.group(1));
    }
    return password;
  }

  private static Optional<String> getMasterPassword(State state) {
    if (!state.contains(ConfigurationKeys.ENCRYPT_KEY_LOC)) {
      LOG.warn(String.format("Property %s not set. Cannot decrypt any encrypted password.",
          ConfigurationKeys.ENCRYPT_KEY_LOC));
      return Optional.absent();
    }
    try {
      if (state.contains(ConfigurationKeys.ENCRYPT_KEY_FS_URI)) {
        FileSystem fs =
            FileSystem.get(URI.create(state.getProp(ConfigurationKeys.ENCRYPT_KEY_FS_URI)), new Configuration());
        return getMasterPassword(fs, new Path(state.getProp(ConfigurationKeys.ENCRYPT_KEY_LOC)));
      }
      return getMasterPassword(new Path(state.getProp(ConfigurationKeys.ENCRYPT_KEY_LOC)));
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to obtain master password from " + state.getProp(ConfigurationKeys.ENCRYPT_KEY_LOC), e);
    }
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
}
