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

package gobblin.source.extractor.extract.sftp;

import gobblin.configuration.State;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.util.HadoopUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.google.common.collect.Lists;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

/**
 * A {@link FileSystem} implementation that provides the {@link FileSystem} interface for an SFTP server.
 * Uses {@link SftpFsHelper} internally to connect to the SFPT server.
 */
public class SftpLightWeightFileSystem extends FileSystem {

  private static final URI NAME = URI.create("sftp:///");
  private SftpFsHelper fsHelper;

  private static final PathFilter VALID_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      if (path == null) {
        return false;
      }
      if (StringUtils.isBlank(path.toString())) {
        return false;
      }
      if (path.toString().equals(".")) {
        return false;
      }
      if (path.toString().equals("..")) {
        return false;
      }
      return true;
    }
  };

  @Override
  public void initialize(URI name, Configuration conf)
      throws IOException {
    super.initialize(name, conf);
    State state = HadoopUtils.getStateFromConf(conf);
    fsHelper = new SftpFsHelper(state);
    try {
      fsHelper.connect();
    } catch (FileBasedHelperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean delete(Path path) throws IOException {

    try {
      if (getFileStatus(path).isDir()) {
        fsHelper.getSftpConnection().rmdir(path.toString());
      } else {
        fsHelper.getSftpConnection().rm(path.toString());
      }
    } catch (SftpException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    if (!recursive) {
      throw new UnsupportedOperationException("Non recursive delete is not supported on this file system");
    }
    return delete(path);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {

    try {
      SftpATTRS sftpAttrs = fsHelper.getSftpConnection().stat(path.toString());
      FsPermission permission = new FsPermission((short) sftpAttrs.getPermissions());
      FileStatus fs =
          new FileStatus(sftpAttrs.getSize(), sftpAttrs.isDir(), 1, 0l, (long) sftpAttrs.getMTime(),
              (long) sftpAttrs.getATime(), permission, Integer.toString(sftpAttrs.getUId()), Integer.toString(sftpAttrs
                  .getGId()), path);

      return fs;
    } catch (SftpException e) {
      throw new IOException(e);
    }

  }

  @Override
  public URI getUri() {
    return NAME;
  }

  @Override
  public Path getWorkingDirectory() {
    try {
      return new Path(fsHelper.getSftpConnection().pwd());
    } catch (SftpException e) {
      return null;
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {

    try {
      List<String> fileNames = fsHelper.ls(path.toString());
      List<FileStatus> status = Lists.newArrayListWithCapacity(fileNames.size());
      for (String name : fileNames) {
        Path filePath = new Path(name);
        if (VALID_PATH_FILTER.accept(filePath)) {
          status.add(getFileStatus(new Path(path,filePath)));
        }
      }
      return status.toArray(new FileStatus[status.size()]);
    } catch (FileBasedHelperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    try {
      fsHelper.getSftpConnection().mkdir(path.toString());
      fsHelper.getSftpConnection().chmod((int) permission.toShort(), path.toString());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    try {
      InputStream is = fsHelper.getFileStream(path.toString());
      return new FSDataInputStream(new BufferedFSInputStream(new SftpFsFileInputStream(is), bufferSize));
    } catch (FileBasedHelperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path path) throws IOException {
    return open(path, 10000);
  }

  @Override
  public boolean rename(Path oldpath, Path newpath) throws IOException {
    try {
      fsHelper.getSftpConnection().rename(oldpath.toString(), newpath.toString());
    } catch (SftpException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    try {
      fsHelper.getSftpConnection().lcd(path.toString());
    } catch (SftpException e) {
      throw new RuntimeException("Failed to set working directory", e);
    }
  }

  @Override
  public void close() {
    fsHelper.close();
  }

  @Override
  public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3, short arg4, long arg5,
      Progressable arg6) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  class SftpFsFileInputStream extends FSInputStream {

    private InputStream in;
    private long pos;

    public SftpFsFileInputStream(InputStream in) {
      this.in = in;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int val = in.read(b, off, len);
      if (val > 0) {
        this.pos += val;
      }
      return val;
    }

    @Override
    public long getPos() throws IOException {
      return this.pos;
    }

    @Override
    public void seek(long pos) throws IOException {
      in.skip(pos);
      this.pos = pos;
    }

    @Override
    public boolean seekToNewSource(long arg0) throws IOException {
      return false;
    }

    @Override
    public int read() throws IOException {
      int val = in.read();
      if (val > 0) {
        this.pos += val;
      }
      return val;
    }
  }
}
