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
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


/**
 * Utility class for creating {@link FileSystem} objects while proxied as another user. This class requires access to a
 * user with secure impersonation priveleges. The {@link FileSystem} objects returned will have full permissions to
 * access any operations on behalf of the specified user.
 *
 * <p>
 *   As a user, use methods in {@link org.apache.gobblin.util.ProxiedFileSystemCache} to generate the proxied file systems.
 * </p>
 *
 * @see <a href="http://hadoop.apache.org/docs/r1.2.1/Secure_Impersonation.html">Secure Impersonation</a>,
 * <a href="https://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/security/UserGroupInformation.html">UserGroupInformation</a>
 *
 * TODO figure out the proper generic type for the {@link Token} objects.
 */
@Slf4j
public class ProxiedFileSystemUtils {

  public static final String AUTH_TYPE_KEY = "gobblin.utility.user.proxy.auth.type";
  public static final String AUTH_TOKEN_PATH = "gobblin.utility.proxy.auth.token.path";

  // Two authentication types for Hadoop Security, through TOKEN or KEYTAB.
  public enum AuthType {
    TOKEN,
    KEYTAB;
  }

  /**
   * Creates a {@link FileSystem} that can perform any operations allowed by the specified userNameToProxyAs.
   *
   * @param userNameToProxyAs The name of the user the super user should proxy as
   * @param properties {@link java.util.Properties} containing initialization properties.
   * @param fsURI The {@link URI} for the {@link FileSystem} that should be created.
   * @param conf The {@link Configuration} for the {@link FileSystem} that should be created.
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   * @throws IOException
   */
  static FileSystem createProxiedFileSystem(@NonNull final String userNameToProxyAs, Properties properties, URI fsURI,
      Configuration conf) throws IOException {
    Preconditions.checkArgument(properties.containsKey(AUTH_TYPE_KEY));

    switch (AuthType.valueOf(properties.getProperty(AUTH_TYPE_KEY))) {
      case TOKEN:
        Preconditions.checkArgument(properties.containsKey(AUTH_TOKEN_PATH));
        Path tokenPath = new Path(properties.getProperty(AUTH_TOKEN_PATH));
        Optional<Token<?>> proxyToken = getTokenFromSeqFile(userNameToProxyAs, tokenPath);
        if (proxyToken.isPresent()) {
          try {
            return createProxiedFileSystemUsingToken(userNameToProxyAs, proxyToken.get(), fsURI, conf);
          } catch (InterruptedException e) {
            throw new IOException("Failed to proxy as user " + userNameToProxyAs, e);
          }
        }
        throw new IOException("No delegation token found for proxy user " + userNameToProxyAs);
      case KEYTAB:
        Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS)
            && properties.containsKey(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION));
        String superUserName = properties.getProperty(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS);
        Path keytabPath = new Path(properties.getProperty(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION));
        try {
          return createProxiedFileSystemUsingKeytab(userNameToProxyAs, superUserName, keytabPath, fsURI, conf);
        } catch (InterruptedException e) {
          throw new IOException("Failed to proxy as user " + userNameToProxyAs, e);
        }
      default:
        throw new IOException("User proxy auth type " + properties.getProperty(AUTH_TYPE_KEY) + " not recognized.");
    }
  }

  /**
   * Creates a {@link FileSystem} that can perform any operations allowed by the specified userNameToProxyAs. This
   * method first logs in as the specified super user. If Hadoop security is enabled, then logging in entails
   * authenticating via Kerberos. So logging in requires contacting the Kerberos infrastructure. A proxy user is then
   * created on behalf of the logged in user, and a {@link FileSystem} object is created using the proxy user's UGI.
   *
   * @param userNameToProxyAs The name of the user the super user should proxy as
   * @param superUserName The name of the super user with secure impersonation priveleges
   * @param superUserKeytabLocation The location of the keytab file for the super user
   * @param fsURI The {@link URI} for the {@link FileSystem} that should be created
   * @param conf The {@link Configuration} for the {@link FileSystem} that should be created
   *
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   */
  static FileSystem createProxiedFileSystemUsingKeytab(String userNameToProxyAs, String superUserName,
      Path superUserKeytabLocation, URI fsURI, Configuration conf) throws IOException, InterruptedException {

    return loginAndProxyAsUser(userNameToProxyAs, superUserName, superUserKeytabLocation)
        .doAs(new ProxiedFileSystem(fsURI, conf));
  }

  /**
   * Create a {@link FileSystem} that can perform any operations allowed the by the specified userNameToProxyAs. This
   * method uses the {@link #createProxiedFileSystemUsingKeytab(String, String, Path, URI, Configuration)} object to perform
   * all its work. A specific set of configuration keys are required to be set in the given {@link State} object:
   *
   * <ul>
   *  <li>{@link ConfigurationKeys#FS_PROXY_AS_USER_NAME} specifies the user name to proxy as</li>
   *  <li>{@link ConfigurationKeys#SUPER_USER_NAME_TO_PROXY_AS_OTHERS} specifies the name of the user with secure
   *  impersonation priveleges</li>
   *  <li>{@link ConfigurationKeys#SUPER_USER_KEY_TAB_LOCATION} specifies the location of the super user's keytab file</li>
   * <ul>
   *
   * @param state The {@link State} object that contains all the necessary key, value pairs for
   * {@link #createProxiedFileSystemUsingKeytab(String, String, Path, URI, Configuration)}
   * @param fsURI The {@link URI} for the {@link FileSystem} that should be created
   * @param conf The {@link Configuration} for the {@link FileSystem} that should be created
   *
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   */
  static FileSystem createProxiedFileSystemUsingKeytab(State state, URI fsURI, Configuration conf)
      throws IOException, InterruptedException {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.FS_PROXY_AS_USER_NAME));
    Preconditions.checkArgument(state.contains(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS));
    Preconditions.checkArgument(state.contains(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION));

    return createProxiedFileSystemUsingKeytab(state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME),
        state.getProp(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS),
        new Path(state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION)), fsURI, conf);
  }

  /**
   * Create a {@link FileSystem} that can perform any operations allowed the by the specified userNameToProxyAs. The
   * method first proxies as userNameToProxyAs, and then adds the specified {@link Token} to the given
   * {@link UserGroupInformation} object. It then uses the {@link UserGroupInformation#doAs(PrivilegedExceptionAction)}
   * method to create a {@link FileSystem}.
   *
   * @param userNameToProxyAs The name of the user the super user should proxy as
   * @param userNameToken The {@link Token} to add to the proxied user's {@link UserGroupInformation}.
   * @param fsURI The {@link URI} for the {@link FileSystem} that should be created
   * @param conf The {@link Configuration} for the {@link FileSystem} that should be created
   *
   * @return a {@link FileSystem} that can execute commands on behalf of the specified userNameToProxyAs
   */
  static FileSystem createProxiedFileSystemUsingToken(@NonNull String userNameToProxyAs,
      @NonNull Token<?> userNameToken, URI fsURI, Configuration conf) throws IOException, InterruptedException {
    UserGroupInformation ugi =
        UserGroupInformation.createProxyUser(userNameToProxyAs, UserGroupInformation.getLoginUser());
    ugi.addToken(userNameToken);
    return ugi.doAs(new ProxiedFileSystem(fsURI, conf));
  }

  /**
   * Returns true if superUserName can proxy as userNameToProxyAs using the specified superUserKeytabLocation, false
   * otherwise.
   */
  public static boolean canProxyAs(String userNameToProxyAs, String superUserName, Path superUserKeytabLocation) {
    try {
      loginAndProxyAsUser(userNameToProxyAs, superUserName, superUserKeytabLocation);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Retrives a {@link Token} from a given sequence file for a specified user. The sequence file should contain a list
   * of key, value pairs where each key corresponds to a user and each value corresponds to a {@link Token} for that
   * user.
   *
   * @param userNameKey The name of the user to retrieve a {@link Token} for
   * @param tokenFilePath The path to the sequence file containing the {@link Token}s
   *
   * @return A {@link Token} for the given user name
   */
  public static Optional<Token<?>> getTokenFromSeqFile(String userNameKey, Path tokenFilePath) throws IOException {
    log.info("Reading tokens from sequence file " + tokenFilePath);

    try (Closer closer = Closer.create()) {
      FileSystem localFs = FileSystem.getLocal(new Configuration());
      @SuppressWarnings("deprecation")
      SequenceFile.Reader tokenReader =
          closer.register(new SequenceFile.Reader(localFs, tokenFilePath, localFs.getConf()));
      Text key = new Text();
      Token<?> value = new Token<>();
      while (tokenReader.next(key, value)) {
        log.debug("Found token for user: " + key);
        if (key.toString().equals(userNameKey)) {
          return Optional.<Token<?>> of(value);
        }
      }
    }
    log.warn("Did not find any tokens for user " + userNameKey);
    return Optional.absent();
  }

  private static UserGroupInformation loginAndProxyAsUser(@NonNull String userNameToProxyAs,
      @NonNull String superUserName, Path superUserKeytabLocation) throws IOException {

    if (!UserGroupInformation.getLoginUser().getUserName().equals(superUserName)) {
      Preconditions.checkNotNull(superUserKeytabLocation);
      UserGroupInformation.loginUserFromKeytab(superUserName, superUserKeytabLocation.toString());
    }
    return UserGroupInformation.createProxyUser(userNameToProxyAs, UserGroupInformation.getLoginUser());
  }

  @AllArgsConstructor
  private static class ProxiedFileSystem implements PrivilegedExceptionAction<FileSystem> {

    @NonNull
    private URI fsURI;

    @NonNull
    private Configuration conf;

    @Override
    public FileSystem run() throws IOException {

      log.info("Creating a filesystem for user: " + UserGroupInformation.getCurrentUser());
      return FileSystem.get(this.fsURI, this.conf);
    }
  }
}
