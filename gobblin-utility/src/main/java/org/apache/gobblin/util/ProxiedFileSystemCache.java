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

package org.apache.gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


/**
 * A cache for storing a mapping between Hadoop users and user {@link FileSystem} objects.
 *
 * <p>
 *  This classes uses Guava's {@link Cache} for storing the user to {@link FileSystem} mapping, and creates the
 *  {@link FileSystem}s using the {@link ProxiedFileSystemUtils} class.
 * </p>
 *
 * @see Cache
 * @see ProxiedFileSystemUtils
 */
public class ProxiedFileSystemCache {

  private static final String KEY_SEPARATOR = ";";
  private static final String RATE_CONTROLLED_TOKEN = "RateControlled";
  private static final int DEFAULT_MAX_CACHE_SIZE = 1000;

  private static final Cache<String, FileSystem> USER_NAME_TO_FILESYSTEM_CACHE =
      CacheBuilder.newBuilder().maximumSize(DEFAULT_MAX_CACHE_SIZE).build();

  /**
   * Gets a {@link FileSystem} that can perform any operations allowed by the specified userNameToProxyAs.
   *
   * @param userNameToProxyAs The name of the user the super user should proxy as
   * @param properties {@link java.util.Properties} containing initialization properties.
   * @param fsURI The {@link URI} for the {@link FileSystem} that should be created.
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   * @throws IOException
   * @deprecated use {@link #fromProperties}
   */
  @Deprecated
  public static FileSystem getProxiedFileSystem(@NonNull final String userNameToProxyAs, Properties properties,
      URI fsURI) throws IOException {
    return getProxiedFileSystem(userNameToProxyAs, properties, fsURI, new Configuration());
  }

  /**
   * Gets a {@link FileSystem} that can perform any operations allowed by the specified userNameToProxyAs.
   *
   * @param userNameToProxyAs The name of the user the super user should proxy as
   * @param properties {@link java.util.Properties} containing initialization properties.
   * @param conf The {@link Configuration} for the {@link FileSystem} that should be created.
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   * @throws IOException
   * @deprecated use {@link #fromProperties}
   */
  @Deprecated
  public static FileSystem getProxiedFileSystem(@NonNull final String userNameToProxyAs, Properties properties,
      Configuration conf) throws IOException {
    return getProxiedFileSystem(userNameToProxyAs, properties, FileSystem.getDefaultUri(conf), conf);
  }

  /**
   * Gets a {@link FileSystem} that can perform any operations allowed by the specified userNameToProxyAs.
   *
   * @param userNameToProxyAs The name of the user the super user should proxy as
   * @param properties {@link java.util.Properties} containing initialization properties.
   * @param fsURI The {@link URI} for the {@link FileSystem} that should be created.
   * @param configuration The {@link Configuration} for the {@link FileSystem} that should be created.
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   * @throws IOException
   * @deprecated Use {@link #fromProperties}
   */
  @Deprecated
  public static FileSystem getProxiedFileSystem(@NonNull final String userNameToProxyAs, final Properties properties,
      final URI fsURI, final Configuration configuration) throws IOException {
    return getProxiedFileSystem(userNameToProxyAs, properties, fsURI, configuration, null);
  }

  /**
   * Gets a {@link FileSystem} that can perform any operations allowed by the specified userNameToProxyAs.
   *
   * @param userNameToProxyAs The name of the user the super user should proxy as
   * @param properties {@link java.util.Properties} containing initialization properties.
   * @param fsURI The {@link URI} for the {@link FileSystem} that should be created.
   * @param configuration The {@link Configuration} for the {@link FileSystem} that should be created.
   * @param referenceFS reference {@link FileSystem}. Used to replicate certain decorators of the reference FS:
   *                    {@link RateControlledFileSystem}.
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   * @throws IOException
   */
  @Builder(builderClassName = "ProxiedFileSystemFromProperties", builderMethodName = "fromProperties")
  private static FileSystem getProxiedFileSystem(@NonNull String userNameToProxyAs, Properties properties, URI fsURI,
      Configuration configuration, FileSystem referenceFS) throws IOException {
    Preconditions.checkNotNull(userNameToProxyAs, "Must provide a user name to proxy as.");
    Preconditions.checkNotNull(properties, "Properties is a mandatory field for proxiedFileSystem generation.");
    URI actualURI = resolveUri(fsURI, configuration, referenceFS);
    Configuration actualConfiguration = resolveConfiguration(configuration, referenceFS);

    try {
      return USER_NAME_TO_FILESYSTEM_CACHE.get(getFileSystemKey(actualURI, userNameToProxyAs, referenceFS),
          new CreateProxiedFileSystemFromProperties(userNameToProxyAs, properties, actualURI, actualConfiguration,
              referenceFS));
    } catch (ExecutionException ee) {
      throw new IOException("Failed to get proxied file system for user " + userNameToProxyAs, ee);
    }
  }

  /**
   * Cached version of {@link ProxiedFileSystemUtils#createProxiedFileSystemUsingKeytab(State, URI, Configuration)}.
   * @deprecated use {@link #fromKeytab}.
   */
  @Deprecated
  public static FileSystem getProxiedFileSystemUsingKeytab(State state, URI fsURI, Configuration conf)
      throws ExecutionException {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.FS_PROXY_AS_USER_NAME));
    Preconditions.checkArgument(state.contains(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS));
    Preconditions.checkArgument(state.contains(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION));

    return getProxiedFileSystemUsingKeytab(state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME),
        state.getProp(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS),
        new Path(state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION)), fsURI, conf);
  }

  /**
   * Cached version of {@link ProxiedFileSystemUtils#createProxiedFileSystemUsingKeytab(String, String, Path, URI, Configuration)}.
   * @deprecated use {@link #fromKeytab}.
   */
  @Deprecated
  public static FileSystem getProxiedFileSystemUsingKeytab(@NonNull final String userNameToProxyAs,
      final String superUserName, final Path superUserKeytabLocation, final URI fsURI, final Configuration conf)
      throws ExecutionException {
    try {
      return getProxiedFileSystemUsingKeytab(userNameToProxyAs, superUserName, superUserKeytabLocation, fsURI, conf,
          null);
    } catch (IOException ioe) {
      throw new ExecutionException(ioe);
    }
  }

  /**
   * Cached version of {@link ProxiedFileSystemUtils#createProxiedFileSystemUsingKeytab(String, String, Path, URI, Configuration)}.
   */
  @Builder(builderClassName = "ProxiedFileSystemFromKeytab", builderMethodName = "fromKeytab")
  private static FileSystem getProxiedFileSystemUsingKeytab(@NonNull final String userNameToProxyAs,
      final String superUserName, final Path superUserKeytabLocation, final URI fsURI, final Configuration conf,
      FileSystem referenceFS) throws IOException, ExecutionException {
    Preconditions.checkNotNull(userNameToProxyAs, "Must provide a user name to proxy as.");
    Preconditions.checkNotNull(superUserName, "Must provide a super user name.");
    Preconditions.checkNotNull(superUserKeytabLocation, "Must provide a keytab location.");
    URI actualURI = resolveUri(fsURI, conf, referenceFS);
    Configuration actualConfiguration = resolveConfiguration(conf, referenceFS);

    return USER_NAME_TO_FILESYSTEM_CACHE.get(getFileSystemKey(actualURI, userNameToProxyAs, referenceFS),
        new CreateProxiedFileSystemFromKeytab(userNameToProxyAs, superUserName, superUserKeytabLocation, actualURI,
            actualConfiguration, referenceFS));
  }

  /**
   * Cached version of {@link ProxiedFileSystemUtils#createProxiedFileSystemUsingToken(String, Token, URI, Configuration)}.
   * @deprecated use {@link #fromToken}.
   */
  @Deprecated
  public static FileSystem getProxiedFileSystemUsingToken(@NonNull final String userNameToProxyAs,
      final Token<?> userNameToken, final URI fsURI, final Configuration conf) throws ExecutionException {
    try {
      return getProxiedFileSystemUsingToken(userNameToProxyAs, userNameToken, fsURI, conf, null);
    } catch (IOException ioe) {
      throw new ExecutionException(ioe);
    }
  }

  /**
   * Cached version of {@link ProxiedFileSystemUtils#createProxiedFileSystemUsingToken(String, Token, URI, Configuration)}.
   */
  @Builder(builderClassName = "ProxiedFileSystemFromToken", builderMethodName = "fromToken")
  private static FileSystem getProxiedFileSystemUsingToken(@NonNull String userNameToProxyAs, Token<?> userNameToken,
      URI fsURI, Configuration conf, FileSystem referenceFS) throws IOException, ExecutionException {
    Preconditions.checkNotNull(userNameToProxyAs, "Must provide a user name to proxy as.");
    Preconditions.checkNotNull(userNameToken, "Must provide token for user to proxy.");
    URI actualURI = resolveUri(fsURI, conf, referenceFS);
    Configuration actualConfiguration = resolveConfiguration(conf, referenceFS);

    return USER_NAME_TO_FILESYSTEM_CACHE.get(getFileSystemKey(actualURI, userNameToProxyAs, referenceFS),
        new CreateProxiedFileSystemFromToken(userNameToProxyAs, userNameToken, actualURI, actualConfiguration,
            referenceFS));
  }

  @AllArgsConstructor
  private static class CreateProxiedFileSystemFromProperties implements Callable<FileSystem> {
    @NonNull
    private final String userNameToProxyAs;
    @NonNull
    private final Properties properties;
    @NonNull
    private final URI uri;
    @NonNull
    private final Configuration configuration;
    private final FileSystem referenceFS;

    @Override
    public FileSystem call() throws Exception {
      FileSystem fs = ProxiedFileSystemUtils.createProxiedFileSystem(this.userNameToProxyAs, this.properties, this.uri,
          this.configuration);
      if (this.referenceFS != null) {
        return decorateFilesystemFromReferenceFS(fs, this.referenceFS);
      }
      return fs;
    }
  }

  @AllArgsConstructor
  private static class CreateProxiedFileSystemFromKeytab implements Callable<FileSystem> {
    @NonNull
    private final String userNameToProxyAs;
    @NonNull
    private final String superUser;
    @NonNull
    private final Path keytabLocation;
    @NonNull
    private final URI uri;
    @NonNull
    private final Configuration configuration;
    private final FileSystem referenceFS;

    @Override
    public FileSystem call() throws Exception {
      FileSystem fs = ProxiedFileSystemUtils.createProxiedFileSystemUsingKeytab(this.userNameToProxyAs, this.superUser,
          this.keytabLocation, this.uri, this.configuration);
      if (this.referenceFS != null) {
        return decorateFilesystemFromReferenceFS(fs, this.referenceFS);
      }
      return fs;
    }
  }

  @AllArgsConstructor
  private static class CreateProxiedFileSystemFromToken implements Callable<FileSystem> {
    @NonNull
    private final String userNameToProxyAs;
    @NonNull
    private final Token<?> userNameToken;
    @NonNull
    private final URI uri;
    @NonNull
    private final Configuration configuration;
    private final FileSystem referenceFS;

    @Override
    public FileSystem call() throws Exception {
      FileSystem fs = ProxiedFileSystemUtils.createProxiedFileSystemUsingToken(this.userNameToProxyAs,
          this.userNameToken, this.uri, this.configuration);
      if (this.referenceFS != null) {
        return decorateFilesystemFromReferenceFS(fs, this.referenceFS);
      }
      return fs;
    }
  }

  private static URI resolveUri(URI uri, Configuration configuration, FileSystem fileSystem) throws IOException {
    if (uri != null) {
      return uri;
    }
    if (fileSystem != null) {
      return fileSystem.getUri();
    }
    if (configuration != null) {
      return FileSystem.getDefaultUri(configuration);
    }
    throw new IOException("FileSystem URI could not be determined from available inputs.");
  }

  private static Configuration resolveConfiguration(Configuration configuration, FileSystem fileSystem)
      throws IOException {
    if (configuration != null) {
      return configuration;
    }
    if (fileSystem != null) {
      return fileSystem.getConf();
    }
    throw new IOException("FileSystem configuration could not be determined from available inputs.");
  }

  private static String getFileSystemKey(URI uri, String user, FileSystem referenceFS) {

    StringBuilder keyBuilder = new StringBuilder();
    keyBuilder.append(uri.toString());
    keyBuilder.append(KEY_SEPARATOR);
    keyBuilder.append(user);

    if (referenceFS != null && RateControlledFileSystem.getRateIfRateControlled(referenceFS).isPresent()) {
      keyBuilder.append(KEY_SEPARATOR);
      keyBuilder.append(RATE_CONTROLLED_TOKEN);
    }

    return keyBuilder.toString();
  }

  private static FileSystem decorateFilesystemFromReferenceFS(FileSystem newFS, FileSystem referenceFS) {
    FileSystem decoratedFs = newFS;

    Optional<Long> decoratedFSRateOpt = RateControlledFileSystem.getRateIfRateControlled(decoratedFs);
    if (!decoratedFSRateOpt.isPresent()) {
      Optional<Long> referenceRateOpt = RateControlledFileSystem.getRateIfRateControlled(referenceFS);
      if (referenceRateOpt.isPresent()) {
        decoratedFs = new RateControlledFileSystem(decoratedFs, referenceRateOpt.get());
      }
    }

    return decoratedFs;
  }
}
