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

package org.apache.gobblin.source.extractor.extract.sftp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;
import com.jcraft.jsch.UserInfo;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.TimestampAwareFileBasedHelper;
import org.apache.gobblin.util.io.SeekableFSInputStream;


/**
 * Connects to a source via SFTP and executes a given list of SFTP commands
 * @author stakiar
 */
@Slf4j
public class SftpFsHelper implements TimestampAwareFileBasedHelper {
  private Session session;
  private State state;

  public SftpFsHelper(State state) {

    this.state = state;
  }

  /**
   * The method returns a new {@link ChannelSftp} without throwing an exception. Returns a null if any exception occurs
   * trying to get a new channel. The method exists for backward compatibility
   *
   * @deprecated use {@link #getSftpChannel()} instead.
   *
   * @return
   */
  @Deprecated
  public ChannelSftp getSftpConnection() {
    try {
      return this.getSftpChannel();
    } catch (SftpException e) {
      log.error("Failed to get new sftp channel", e);
      return null;
    }
  }

  /**
   * Create new channel every time a command needs to be executed. This is required to support execution of multiple
   * commands in parallel. All created channels are cleaned up when the session is closed.
   *
   *
   * @return a new {@link ChannelSftp}
   * @throws SftpException
   */
  public ChannelSftp getSftpChannel() throws SftpException {

    try {
      ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
      channelSftp.connect();
      return channelSftp;
    } catch (JSchException e) {
      throw new SftpException(0, "Cannot open a channel to SFTP server", e);
    }
  }

  /**
   * Create a new sftp channel to execute commands.
   *
   * @param command to execute on the remote machine
   * @return a new execution channel
   * @throws SftpException if a channel could not be opened
   */
  public ChannelExec getExecChannel(String command) throws SftpException {
    ChannelExec channelExec;
    try {
      channelExec = (ChannelExec) this.session.openChannel("exec");
      channelExec.setCommand(command);
      channelExec.connect();
      return channelExec;
    } catch (JSchException e) {
      throw new SftpException(0, "Cannot open a channel to SFTP server", e);
    }
  }

  /**
   * Opens up a connection to specified host using the username. Connects to the source using a private key without
   * prompting for a password. This method does not support connecting to a source using a password, only by private
   * key
   * @throws org.apache.gobblin.source.extractor.filebased.FileBasedHelperException
   */
  @Override
  public void connect() throws FileBasedHelperException {

    String privateKey = PasswordManager.getInstance(this.state)
        .readPassword(this.state.getProp(ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY));
    String password = PasswordManager.getInstance(this.state)
        .readPassword(this.state.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
    String knownHosts = this.state.getProp(ConfigurationKeys.SOURCE_CONN_KNOWN_HOSTS);

    String userName = this.state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
    String hostName = this.state.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    int port = this.state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_PORT, ConfigurationKeys.SOURCE_CONN_DEFAULT_PORT);

    String proxyHost = this.state.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL);
    int proxyPort = this.state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, -1);

    JSch.setLogger(new JSchLogger());
    JSch jsch = new JSch();

    log.info("Attempting to connect to source via SFTP with" + " privateKey: " + privateKey + " knownHosts: "
        + knownHosts + " userName: " + userName + " hostName: " + hostName + " port: " + port + " proxyHost: "
        + proxyHost + " proxyPort: " + proxyPort);

    try {

      if (!Strings.isNullOrEmpty(privateKey)) {
        List<IdentityStrategy> identityStrategies = ImmutableList.of(new LocalFileIdentityStrategy(),
            new DistributedCacheIdentityStrategy(), new HDFSIdentityStrategy());

        for (IdentityStrategy identityStrategy : identityStrategies) {
          if (identityStrategy.setIdentity(privateKey, jsch)) {
            break;
          }
        }
      }

      this.session = jsch.getSession(userName, hostName, port);
      this.session.setConfig("PreferredAuthentications", "publickey,password");

      if (Strings.isNullOrEmpty(knownHosts)) {
        log.info("Known hosts path is not set, StrictHostKeyChecking will be turned off");
        this.session.setConfig("StrictHostKeyChecking", "no");
      } else {
        jsch.setKnownHosts(knownHosts);
      }

      if (!Strings.isNullOrEmpty(password)) {
        this.session.setPassword(password);
      }

      if (proxyHost != null && proxyPort >= 0) {
        this.session.setProxy(new ProxyHTTP(proxyHost, proxyPort));
      }

      UserInfo ui = new MyUserInfo();
      this.session.setUserInfo(ui);
      this.session.setDaemonThread(true);
      this.session.connect();

      log.info("Finished connecting to source");
    } catch (JSchException e) {
      if (this.session != null) {
        this.session.disconnect();
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
  @Override
  public InputStream getFileStream(String file) throws FileBasedHelperException {
    SftpGetMonitor monitor = new SftpGetMonitor();
    try {
      ChannelSftp channel = getSftpChannel();
      return new SftpFsFileInputStream(channel.get(file, monitor), channel);
    } catch (SftpException e) {
      throw new FileBasedHelperException("Cannot download file " + file + " due to " + e.getMessage(), e);
    }
  }

  @Override
  public List<String> ls(String path) throws FileBasedHelperException {
    try {
      List<String> list = new ArrayList<>();
      ChannelSftp channel = getSftpChannel();
      Vector<LsEntry> vector = channel.ls(path);
      for (LsEntry entry : vector) {
        list.add(entry.getFilename());
      }
      channel.disconnect();
      return list;
    } catch (SftpException e) {
      throw new FileBasedHelperException("Cannot execute ls command on sftp connection", e);
    }
  }

  @Override
  public void close() {
    if (this.session != null) {
      this.session.disconnect();
    }
  }

  @Override
  public long getFileSize(String filePath) throws FileBasedHelperException {
    try {
      ChannelSftp channelSftp = getSftpChannel();
      long fileSize = channelSftp.lstat(filePath).getSize();
      channelSftp.disconnect();
      return fileSize;
    } catch (SftpException e) {
      throw new FileBasedHelperException(
          String.format("Failed to get size for file at path %s due to error %s", filePath, e.getMessage()), e);
    }
  }

  /**
   * Implementation of an SftpProgressMonitor to monitor the progress of file downloads using the ChannelSftp.GET
   * methods
   * @author stakiar
   */
  public static class SftpGetMonitor implements SftpProgressMonitor {

    private int op;
    private String src;
    private String dest;
    private long totalCount;
    private long logFrequency;
    private long startime;

    @Override
    public void init(int op, String src, String dest, long max) {
      this.op = op;
      this.src = src;
      this.dest = dest;
      this.startime = System.currentTimeMillis();
      this.logFrequency = 0L;
      log.info("Operation GET (" + op + ") has started with src: " + src + " dest: " + dest + " and file length: "
          + (max / 1000000L) + " mb");
    }

    @Override
    public boolean count(long count) {
      this.totalCount += count;

      if (this.logFrequency == 0L) {
        this.logFrequency = 1000L;
        log.info(
            "Transfer is in progress for file: " + this.src + ". Finished transferring " + this.totalCount + " bytes ");
        long mb = this.totalCount / 1000000L;
        log.info("Transferd " + mb + " Mb. Speed " + getMbps() + " Mbps");
      }
      this.logFrequency--;
      return true;
    }

    @Override
    public void end() {
      long secs = (System.currentTimeMillis() - this.startime) / 1000L;
      log.info("Transfer finished " + this.op + " src: " + this.src + " dest: " + this.dest + " in " + secs + " at "
          + getMbps());
    }

    private String getMbps() {
      long mb = this.totalCount / 1000000L;
      long secs = (System.currentTimeMillis() - this.startime) / 1000L;
      double mbps = secs == 0L ? 0.0D : mb * 1.0D / secs;
      return String.format("%.2f", new Object[] { Double.valueOf(mbps) });
    }
  }

  /**
   * Basic implementation of jsch.Logger that logs the output from the JSch commands to slf4j
   * @author stakiar
   */
  public static class JSchLogger implements com.jcraft.jsch.Logger {
    @Override
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
        default:
          return false;
      }
    }

    @Override
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
        default:
          log.info(message);
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

  /**
   * Interface for multiple identity setter strategies
   */
  private interface IdentityStrategy {
    public boolean setIdentity(String privateKey, JSch jsch);
  }

  /**
   * Sets identity using a file on HDFS
   */
  private static class HDFSIdentityStrategy implements IdentityStrategy {
    @Override
    public boolean setIdentity(String privateKey, JSch jsch) {

      FileSystem fs;
      try {
        fs = FileSystem.get(new Configuration());
      } catch (Exception e) {
        log.warn("Failed to set identity using HDFS file. Will attempt next strategy. " + e.getMessage());
        return false;
      }

      Preconditions.checkNotNull(fs, "FileSystem cannot be null");
      try (FSDataInputStream privateKeyStream = fs.open(new Path(privateKey))) {
        byte[] bytes = IOUtils.toByteArray(privateKeyStream);
        jsch.addIdentity("sftpIdentityKey", bytes, (byte[]) null, (byte[]) null);
        log.info("Successfully set identity using HDFS file");
        return true;
      } catch (Exception e) {
        log.warn("Failed to set identity using HDFS file. Will attempt next strategy. " + e.getMessage());
        return false;
      }
    }
  }

  /**
   * Sets identity using a local file
   */
  private static class LocalFileIdentityStrategy implements IdentityStrategy {
    @Override
    public boolean setIdentity(String privateKey, JSch jsch) {
      try {
        jsch.addIdentity(privateKey);
        log.info("Successfully set identity using local file " + privateKey);
        return true;
      } catch (Exception e) {
        log.warn("Failed to set identity using local file. Will attempt next strategy. " + e.getMessage());
      }
      return false;
    }
  }

  /**
   * Sets identity using a file on distributed cache
   */
  private static class DistributedCacheIdentityStrategy extends LocalFileIdentityStrategy {
    @Override
    public boolean setIdentity(String privateKey, JSch jsch) {
      return super.setIdentity(new File(privateKey).getName(), jsch);
    }
  }

  @Override
  public long getFileMTime(String filePath) throws FileBasedHelperException {
      ChannelSftp channelSftp = null;
      try {
	  channelSftp = getSftpChannel();
	  int modificationTime = channelSftp.lstat(filePath).getMTime();
	  return modificationTime;
      } catch (SftpException e) {
	  throw new FileBasedHelperException(
					     String.format("Failed to get modified timestamp for file at path %s due to error %s", filePath,
							   e.getMessage()),
					     e);
      } finally {
	  if (channelSftp != null) {
	      channelSftp.disconnect();
	  }
      }
  }

  /**
   * A {@link SeekableFSInputStream} that holds a handle on the Sftp {@link Channel} used to open the
   * {@link InputStream}. The {@link Channel} is disconnected when {@link InputStream#close()} is called.
   */
  static class SftpFsFileInputStream extends SeekableFSInputStream {

    private final Channel channel;

    public SftpFsFileInputStream(InputStream in, Channel channel) {
      super(in);
      this.channel = channel;
    }

    @Override
    public void close() throws IOException {
      super.close();
      this.channel.disconnect();
    }
  }
}
