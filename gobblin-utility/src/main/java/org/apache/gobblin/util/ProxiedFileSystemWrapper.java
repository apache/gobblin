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

package gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * A wrapper class for generating a file system as a proxy user.
 */
@Deprecated
public class ProxiedFileSystemWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(ProxiedFileSystemWrapper.class);
  private FileSystem proxiedFs;

  /**
   * Two authentication types for Hadoop Security, through TOKEN or KEYTAB.
   * @deprecated Use {@link gobblin.util.ProxiedFileSystemUtils.AuthType}.
   */
  @Deprecated
  public enum AuthType {
    TOKEN,
    KEYTAB;
  }

  /**
   * Setter for proxiedFs.
   * @param currentProxiedFs
   */
  public void setProxiedFileSystem(FileSystem currentProxiedFs) {
    this.proxiedFs = currentProxiedFs;
  }

  /**
   * Same as @see #getProxiedFileSystem(State, AuthType, String, String, Configuration) where state properties will be copied
   * into Configuration.
   *
   * @param properties
   * @param authType
   * @param authPath
   * @param uri
   * @return
   * @throws IOException
   * @throws InterruptedException
   * @throws URISyntaxException
   */
  public FileSystem getProxiedFileSystem(State properties, AuthType authType, String authPath, String uri)
      throws IOException, InterruptedException, URISyntaxException {
    Configuration conf = new Configuration();
    JobConfigurationUtils.putStateIntoConfiguration(properties, conf);
    return getProxiedFileSystem(properties, authType, authPath, uri, conf);
  }

  /**
   * Getter for proxiedFs, using the passed parameters to create an instance of a proxiedFs.
   * @param properties
   * @param authType is either TOKEN or KEYTAB.
   * @param authPath is the KEYTAB location if the authType is KEYTAB; otherwise, it is the token file.
   * @param uri File system URI.
   * @throws IOException
   * @throws InterruptedException
   * @throws URISyntaxException
   * @return proxiedFs
   */
  public FileSystem getProxiedFileSystem(State properties, AuthType authType, String authPath, String uri, final Configuration conf)
      throws IOException, InterruptedException, URISyntaxException {
    Preconditions.checkArgument(StringUtils.isNotBlank(properties.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME)),
        "State does not contain a proper proxy user name");
    String proxyUserName = properties.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME);
    UserGroupInformation proxyUser;
    switch (authType) {
      case KEYTAB: // If the authentication type is KEYTAB, log in a super user first before creating a proxy user.
        Preconditions.checkArgument(
            StringUtils.isNotBlank(properties.getProp(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS)),
            "State does not contain a proper proxy token file name");
        String superUser = properties.getProp(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS);
        UserGroupInformation.loginUserFromKeytab(superUser, authPath);
        proxyUser = UserGroupInformation.createProxyUser(proxyUserName, UserGroupInformation.getLoginUser());
        break;
      case TOKEN: // If the authentication type is TOKEN, create a proxy user and then add the token to the user.
        proxyUser = UserGroupInformation.createProxyUser(proxyUserName, UserGroupInformation.getLoginUser());
        Optional<Token<?>> proxyToken = getTokenFromSeqFile(authPath, proxyUserName);
        if (proxyToken.isPresent()) {
          proxyUser.addToken(proxyToken.get());
        } else {
          LOG.warn("No delegation token found for the current proxy user.");
        }
        break;
      default:
        LOG.warn("Creating a proxy user without authentication, which could not perform File system operations.");
        proxyUser = UserGroupInformation.createProxyUser(proxyUserName, UserGroupInformation.getLoginUser());
        break;
    }

    final URI fsURI = URI.create(uri);
    proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws IOException {
        LOG.debug("Now performing file system operations as :" + UserGroupInformation.getCurrentUser());
        proxiedFs = FileSystem.get(fsURI, conf);
        return null;
      }
    });
    return this.proxiedFs;
  }

  /**
   * Get token from the token sequence file.
   * @param authPath
   * @param proxyUserName
   * @return Token for proxyUserName if it exists.
   * @throws IOException
   */
  private static Optional<Token<?>> getTokenFromSeqFile(String authPath, String proxyUserName) throws IOException {
    try (Closer closer = Closer.create()) {
      FileSystem localFs = FileSystem.getLocal(new Configuration());
      SequenceFile.Reader tokenReader =
          closer.register(new SequenceFile.Reader(localFs, new Path(authPath), localFs.getConf()));
      Text key = new Text();
      Token<?> value = new Token<>();
      while (tokenReader.next(key, value)) {
        LOG.info("Found token for " + key);
        if (key.toString().equals(proxyUserName)) {
          return Optional.<Token<?>> of(value);
        }
      }
    }
    return Optional.absent();
  }
}
