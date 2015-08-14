/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.helix.api.id.ParticipantId;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;


/**
 * A utility class for Gobblin on Yarn/Helix.
 *
 * @author ynli
 */
public class YarnHelixUtils {

  public static String getHostname() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }

  /**
   * Get a Helix {@link ParticipantId} from a given host name and a given Yarn {@link ContainerId}.
   *
   * @param hostName the given host name
   * @param containerId the given {@link ContainerId}
   * @return a Helix {@link ParticipantId}
   */
  public static ParticipantId getParticipantId(String hostName, ContainerId containerId) {
    return ParticipantId.from(getParticipantIdStr(hostName, containerId));
  }

  /**
   * Get the string form of a Helix {@link ParticipantId} from a given host name and a given Yarn
   * {@link ContainerId}.
   *
   * @param hostName the given host name
   * @param containerId the given {@link ContainerId}
   * @return the string form of a Helix {@link ParticipantId}
   */
  public static String getParticipantIdStr(String hostName, ContainerId containerId) {
    return hostName + "_" + containerId.toString();
  }

  /**
   * Get the Yarn application working directory {@link Path}.
   *
   * @param fs a {@link FileSystem} instance on which {@link FileSystem#getHomeDirectory()} is called
   *           to get the home directory of the {@link FileSystem} of the application working directory
   * @param applicationName the Yarn application name
   * @param applicationId the Yarn {@link ApplicationId}
   * @return the Yarn application working directory {@link Path}
   */
  public static Path getAppWorkDirPath(FileSystem fs, String applicationName, ApplicationId applicationId) {
    return new Path(fs.getHomeDirectory(), applicationName + Path.SEPARATOR + applicationId.toString());
  }

  /**
   * Convert a given {@link Config} instance to a {@link Properties} instance.
   *
   * @param config the given {@link Config} instance
   * @return a {@link Properties} instance
   */
  public static Properties configToProperties(Config config) {
    Properties properties = new Properties();
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      properties.setProperty(entry.getKey(), config.getString(entry.getKey()));
    }

    return properties;
  }

  /**
   * Write a {@link Token} to a given file.
   *
   * @param token the token to write
   * @param tokenFilePath the token file path
   * @param configuration a {@link Configuration} object carrying Hadoop configuration properties
   * @throws IOException
   */
  public static void writeTokenToFile(Token<? extends TokenIdentifier> token, Path tokenFilePath,
      Configuration configuration) throws IOException {
    Credentials credentials = new Credentials();
    credentials.addToken(token.getService(), token);
    credentials.writeTokenStorageFile(tokenFilePath, configuration);
  }

  /**
   * Read a collection {@link Token}s from a given file.
   *
   * @param tokenFilePath the token file path
   * @param configuration a {@link Configuration} object carrying Hadoop configuration properties
   * @return a collection of {@link Token}s
   * @throws IOException
   */
  public static Collection<Token<? extends TokenIdentifier>> readTokensFromFile(Path tokenFilePath,
      Configuration configuration) throws IOException {
    return Credentials.readTokenStorageFile(tokenFilePath, configuration).getAllTokens();
  }
}
