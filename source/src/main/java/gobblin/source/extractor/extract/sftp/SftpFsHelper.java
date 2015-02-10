/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.sftp;

import gobblin.source.extractor.filebased.FileBasedHelper;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;
import com.jcraft.jsch.UserInfo;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * Connects to a source via SFTP and executes a given list of SFTP commands
 * @author stakiar
 */
public class SftpFsHelper implements FileBasedHelper {
  private static Logger log = LoggerFactory.getLogger(SftpFsHelper.class);
  private ChannelSftp channelSftp;
  private Session session;
  private State state;

  public SftpFsHelper(State state) {
    this.state = state;
  }

  public ChannelSftp getSftpConnection() {
    return this.channelSftp;
  }

  /**
   * Opens up a connection to specified host using the username. Connects to the source using a private key without
   * prompting for a password. This method does not support connecting to a source using a password, only by private
   * key
   * @throws gobblin.source.extractor.filebased.FileBasedHelperException
   */
  @Override
  public void connect()
      throws FileBasedHelperException {
    String privateKey = state.getProp(ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY);
    String knownHosts = state.getProp(ConfigurationKeys.SOURCE_CONN_KNOWN_HOSTS);

    String userName = state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
    String hostName = state.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    int port = state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_PORT, ConfigurationKeys.SOURCE_CONN_DEFAULT_PORT);

    String proxyHost = state.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL);
    int proxyPort = state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, -1);

    JSch.setLogger(new JSchLogger());
    JSch jsch = new JSch();

    log.info(
        "Attempting to connect to source via SFTP with" + " privateKey: " + privateKey + " knownHosts: " + knownHosts
            + " userName: " + userName + " hostName: " + hostName + " port: " + port + " proxyHost: " + proxyHost
            + " proxyPort: " + proxyPort);
    try {
      jsch.addIdentity(privateKey);
      jsch.setKnownHosts(knownHosts);

      session = jsch.getSession(userName, hostName, port);

      if (proxyHost != null && proxyPort >= 0) {
        session.setProxy(new ProxyHTTP(proxyHost, proxyPort));
      }

      UserInfo ui = new MyUserInfo();
      session.setUserInfo(ui);

      session.connect();

      channelSftp = (ChannelSftp) session.openChannel("sftp");
      channelSftp.connect();
      log.info("Finished connecting to source");
    } catch (JSchException e) {
      if (session != null) {
        session.disconnect();
      }
      if (channelSftp != null) {
        channelSftp.disconnect();
      }
      log.error(e.getMessage(), e);
      throw new FileBasedHelperException("Cannot connect to SFTP source", e);
    }
  }

  /**
   * Executes a get SftpCommand and returns an input stream to the file
   * @param cmd is the command to execute
   * @param sftp is the channel to execute the command on
   * @throws SftpException
   */
  public InputStream getFileStream(String file)
      throws FileBasedHelperException {
    SftpGetMonitor monitor = new SftpGetMonitor();
    try {
      return this.channelSftp.get(file, monitor);
    } catch (SftpException e) {
      throw new FileBasedHelperException("Cannot download file " + file + " due to " + e.getMessage(), e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> ls(String path)
      throws FileBasedHelperException {
    try {
      List<String> list = new ArrayList<String>();
      Vector<LsEntry> vector = this.channelSftp.ls(path);
      for (LsEntry entry : vector) {
        list.add(entry.getFilename());
      }
      return list;
    } catch (SftpException e) {
      throw new FileBasedHelperException("Cannot execute ls command on sftp connection", e);
    }
  }

  @Override
  public void close() {
    if (session != null) {
      session.disconnect();
    }
    if (channelSftp != null) {
      channelSftp.disconnect();
    }
  }

  /**
   * Implementation of an SftpProgressMonitor to monitor the progress of file downloads using the ChannelSftp.GET
   * methods
   * @author stakiar
   */
  private class SftpGetMonitor implements SftpProgressMonitor {

    private int op;
    private String src;
    private String dest;
    private long totalCount;

    @Override
    public void init(int op, String src, String dest, long max) {
      this.op = op;
      this.src = src;
      this.dest = dest;
      log.info(
          "Operation GET (" + op + ") has started with src: " + src + " dest: " + dest + " and file length: " + max);
    }

    @Override
    public boolean count(long count) {
      this.totalCount += count;
      log.info(
          "Transfer is in progress for file: " + src + ". Finished transferring " + this.totalCount + " bytes " + System
              .currentTimeMillis());
      return true;
    }

    @Override
    public void end() {
      log.info("Data transfer has finished for operation " + this.op + " src: " + this.src + " dest: " + this.dest);
    }
  }

  /**
   * Basic implementation of jsch.Logger that logs the output from the JSch commands to slf4j
   * @author stakiar
   */
  public static class JSchLogger implements com.jcraft.jsch.Logger {
    public boolean isEnabled(int level) {
      switch (level) {
        case DEBUG:
          return log.isDebugEnabled();
        case INFO:
          return log.isInfoEnabled();
        case WARN:
          return log.isWarnEnabled();
        case ERROR:
          return log.isErrorEnabled();
        case FATAL:
          return log.isErrorEnabled();
      }
      return false;
    }

    public void log(int level, String message) {
      switch (level) {
        case DEBUG:
          log.debug(message);
          break;
        case INFO:
          log.info(message);
          break;
        case WARN:
          log.warn(message);
          break;
        case ERROR:
          log.error(message);
          break;
        case FATAL:
          log.error(message);
          break;
      }
    }
  }

  /**
   * Implementation of UserInfo class for JSch which allows for password-less login via keys
   * @author stakiar
   */
  public static class MyUserInfo implements UserInfo {

    // The passphrase used to access the private key
    @Override
    public String getPassphrase() {
      return null;
    }

    // The password to login to the client server
    @Override
    public String getPassword() {
      return null;
    }

    @Override
    public boolean promptPassword(String message) {
      return true;
    }

    @Override
    public boolean promptPassphrase(String message) {
      return true;
    }

    @Override
    public boolean promptYesNo(String message) {
      return true;
    }

    @Override
    public void showMessage(String message) {
      log.info(message);
    }
  }
}
