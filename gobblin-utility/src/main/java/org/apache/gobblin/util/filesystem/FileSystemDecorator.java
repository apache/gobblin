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

package org.apache.gobblin.util.filesystem;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import org.apache.gobblin.util.Decorator;

import static org.apache.gobblin.util.filesystem.InstrumentedFileSystemUtils.*;


/**
 * This is a decorator for {@link FileSystem} that allows optionally changing scheme.
 *
 * Note subclasses must set the underlying {@link FileSystem} at {@link #underlyingFs} as necessary.
 */
class FileSystemDecorator extends FileSystem implements Decorator {

  protected String replacementScheme;
  protected String underlyingScheme;
  protected Configuration conf;
  protected FileSystem underlyingFs;

  FileSystemDecorator(String replacementScheme, String underlyingScheme) {
    this.replacementScheme = replacementScheme;
    this.underlyingScheme = underlyingScheme;
  }

  @Override
  public Object getDecoratedObject() {
    return this.underlyingFs;
  }

  @Override
  public String getScheme() {
    return this.replacementScheme;
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException {
    if (this.underlyingFs == null) {
      throw new IllegalStateException("Underlying fs has not been defined.");
    }
    this.underlyingFs.initialize(replaceScheme(uri, this.replacementScheme, this.underlyingScheme), conf);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    if (this.underlyingFs != null) {
      this.underlyingFs.setConf(conf);
    }
  }

  @Override
  public URI getUri() {
    return replaceScheme(this.underlyingFs.getUri(), this.underlyingScheme, this.replacementScheme);
  }

  public FileStatus getFileLinkStatus(Path f) throws java.io.IOException {
    return replaceScheme(this.underlyingFs.getFileLinkStatus(replaceScheme(f, this.replacementScheme, this.underlyingScheme)),
        this.underlyingScheme, this.replacementScheme);
  }

  public FsStatus getStatus() throws java.io.IOException {
    return this.underlyingFs.getStatus();
  }

  public FSDataOutputStream append(Path f) throws java.io.IOException {
    return this.underlyingFs.append(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public short getReplication(Path src) throws java.io.IOException {
    return this.underlyingFs.getReplication(replaceScheme(src, this.replacementScheme, this.underlyingScheme));
  }

  public void close() throws java.io.IOException {
    this.underlyingFs.close();
  }

  public void setWriteChecksum(boolean writeChecksum) {
    this.underlyingFs.setWriteChecksum(writeChecksum);
  }

  public FileChecksum getFileChecksum(Path f) throws java.io.IOException {
    return this.underlyingFs.getFileChecksum(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public boolean isDirectory(Path f) throws java.io.IOException {
    return this.underlyingFs.isDirectory(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public void createSymlink(Path target, Path link, boolean createParent)
      throws IOException {
    this.underlyingFs.createSymlink(replaceScheme(target, this.replacementScheme, this.underlyingScheme),
        replaceScheme(link, this.replacementScheme, this.underlyingScheme), createParent);
  }

  public Path createSnapshot(Path path, String snapshotName) throws java.io.IOException {
    return replaceScheme(this.underlyingFs.createSnapshot(replaceScheme(path, this.replacementScheme, this.underlyingScheme), snapshotName),
        this.underlyingScheme, this.replacementScheme);
  }

  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
      throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  public Path resolvePath(Path p) throws java.io.IOException {
    return replaceScheme(this.underlyingFs.resolvePath(replaceScheme(p, this.replacementScheme, this.underlyingScheme)), this.underlyingScheme, this.replacementScheme);
  }

  public FileStatus[] listStatus(Path f) throws java.io.FileNotFoundException, java.io.IOException {
    return replaceScheme(
        this.underlyingFs.listStatus(replaceScheme(f, this.replacementScheme, this.underlyingScheme)),
        this.underlyingScheme, this.replacementScheme);
  }

  public long getUsed() throws java.io.IOException {
    return this.underlyingFs.getUsed();
  }

  public Configuration getConf() {
    return this.underlyingFs.getConf();
  }

  public FSDataOutputStream create(Path f, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme), progress);
  }

  public boolean isFile(Path f) throws java.io.IOException {
    return this.underlyingFs.isFile(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public Path getWorkingDirectory() {
    return replaceScheme(this.underlyingFs.getWorkingDirectory(), this.underlyingScheme, this.replacementScheme);
  }

  public FsServerDefaults getServerDefaults() throws java.io.IOException {
    return this.underlyingFs.getServerDefaults();
  }

  public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem)
      throws java.io.IOException {
    this.underlyingFs.copyToLocalFile(delSrc, replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme), useRawLocalFileSystem);
  }

  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return replaceScheme(
        this.underlyingFs.globStatus(replaceScheme(pathPattern, this.replacementScheme, this.underlyingScheme)),
        this.underlyingScheme, this.replacementScheme);
  }

  public void setWorkingDirectory(Path new_dir) {
    this.underlyingFs.setWorkingDirectory(replaceScheme(new_dir, this.replacementScheme, this.underlyingScheme));
  }

  public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
    return replaceScheme(
        this.underlyingFs.listStatus(replaceScheme(f, this.replacementScheme, this.underlyingScheme), filter),
        this.underlyingScheme, this.replacementScheme);
  }

  public String getName() {
    return this.underlyingFs.getName();
  }

  public boolean createNewFile(Path f) throws java.io.IOException {
    return this.underlyingFs.createNewFile(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public FileStatus[] listStatus(Path[] files) throws IOException {
    return replaceScheme(
        this.underlyingFs.listStatus(replaceScheme(files, this.replacementScheme, this.underlyingScheme)),
        this.underlyingScheme, this.replacementScheme);
  }

  public boolean delete(Path f, boolean recursive) throws java.io.IOException {
    return this.underlyingFs.delete(replaceScheme(f, this.replacementScheme, this.underlyingScheme), recursive);
  }

  public Path getLinkTarget(Path f) throws java.io.IOException {
    return replaceScheme(this.underlyingFs.getLinkTarget(replaceScheme(f, this.replacementScheme, this.underlyingScheme)),
        this.underlyingScheme, this.replacementScheme);
  }

  public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws java.io.IOException {
    this.underlyingFs.copyToLocalFile(delSrc, replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public short getDefaultReplication() {
    return this.underlyingFs.getDefaultReplication();
  }

  public Token<?> getDelegationToken(String renewer) throws java.io.IOException {
    return this.underlyingFs.getDelegationToken(renewer);
  }

  public FsServerDefaults getServerDefaults(Path p) throws java.io.IOException {
    return this.underlyingFs.getServerDefaults(replaceScheme(p, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream create(Path f, short replication) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme), replication);
  }

  public boolean mkdirs(Path f, FsPermission permission) throws java.io.IOException {
    return this.underlyingFs.mkdirs(replaceScheme(f, this.replacementScheme, this.underlyingScheme), permission);
  }

  public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws java.io.IOException {
    return this.underlyingFs.getFileBlockLocations(replaceScheme(p, this.replacementScheme, this.underlyingScheme),
        start, len);
  }

  public void concat(Path trg, Path[] psrcs) throws java.io.IOException {
    this.underlyingFs.concat(replaceScheme(trg, this.replacementScheme, this.underlyingScheme),
        replaceScheme(psrcs, this.replacementScheme, this.underlyingScheme));
  }

  public Path getHomeDirectory() {
    return replaceScheme(this.underlyingFs.getHomeDirectory(), this.underlyingScheme, this.replacementScheme);
  }

  public FileStatus getFileStatus(Path f) throws java.io.IOException {
    return replaceScheme(this.underlyingFs.getFileStatus(replaceScheme(f, this.replacementScheme, this.underlyingScheme)),
        this.underlyingScheme, this.replacementScheme);
  }

  public boolean supportsSymlinks() {
    return this.underlyingFs.supportsSymlinks();
  }

  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws java.io.FileNotFoundException, java.io.IOException {
    return this.underlyingFs.listLocatedStatus(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws java.io.IOException {
    return this.underlyingFs.listCorruptFileBlocks(replaceScheme(path, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme), replication, progress);
  }

  public boolean setReplication(Path src, short replication) throws java.io.IOException {
    return this.underlyingFs.setReplication(replaceScheme(src, this.replacementScheme, this.underlyingScheme), replication);
  }

  public Path makeQualified(Path path) {
    return replaceScheme(this.underlyingFs.makeQualified(replaceScheme(path, this.replacementScheme, this.underlyingScheme)), this.underlyingScheme, this.replacementScheme);
  }

  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.createNonRecursive(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  public long getBlockSize(Path f) throws java.io.IOException {
    return this.underlyingFs.getBlockSize(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public short getDefaultReplication(Path path) {
    return this.underlyingFs.getDefaultReplication(replaceScheme(path, this.replacementScheme, this.underlyingScheme));
  }

  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws java.io.IOException {
    return this.underlyingFs.addDelegationTokens(renewer, credentials);
  }

  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        overwrite, bufferSize, replication, blockSize, progress);
  }

  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.append(replaceScheme(f, this.replacementScheme, this.underlyingScheme), bufferSize, progress);
  }

  public void copyToLocalFile(Path src, Path dst) throws java.io.IOException {
    this.underlyingFs.copyToLocalFile(replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataInputStream open(Path f, int bufferSize) throws java.io.IOException {
    return this.underlyingFs.open(replaceScheme(f, this.replacementScheme, this.underlyingScheme), bufferSize);
  }

  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
    return replaceScheme(
        this.underlyingFs.listStatus(replaceScheme(files, this.replacementScheme, this.underlyingScheme), filter),
        this.underlyingScheme, this.replacementScheme);
  }

  public long getLength(Path f) throws java.io.IOException {
    return this.underlyingFs.getLength(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public void moveToLocalFile(Path src, Path dst) throws java.io.IOException {
    this.underlyingFs.moveToLocalFile(replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public FsStatus getStatus(Path p) throws java.io.IOException {
    return this.underlyingFs.getStatus(replaceScheme(p, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.createNonRecursive(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  public String getCanonicalServiceName() {
    return this.underlyingFs.getCanonicalServiceName();
  }

  public boolean cancelDeleteOnExit(Path f) {
    return this.underlyingFs.cancelDeleteOnExit(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws java.io.IOException {
    this.underlyingFs.copyFromLocalFile(delSrc, overwrite, replaceScheme(srcs, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public void setPermission(Path p, FsPermission permission) throws java.io.IOException {
    this.underlyingFs.setPermission(replaceScheme(p, this.replacementScheme, this.underlyingScheme), permission);
  }

  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws java.io.IOException {
    return replaceScheme(this.underlyingFs.startLocalOutput(replaceScheme(fsOutputFile, this.replacementScheme, this.underlyingScheme),
        replaceScheme(tmpLocalFile, this.replacementScheme, this.underlyingScheme)), this.underlyingScheme, this.replacementScheme);
  }

  public void copyFromLocalFile(Path src, Path dst) throws java.io.IOException {
    this.underlyingFs.copyFromLocalFile(replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public boolean delete(Path f) throws java.io.IOException {
    return this.underlyingFs.delete(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public FileSystem[] getChildFileSystems() {
    return this.underlyingFs.getChildFileSystems();
  }

  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws java.io.IOException {
    this.underlyingFs.copyFromLocalFile(delSrc, replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public void setTimes(Path p, long mtime, long atime) throws java.io.IOException {
    this.underlyingFs.setTimes(replaceScheme(p, this.replacementScheme, this.underlyingScheme), mtime, atime);
  }

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws java.io.IOException {
    return this.underlyingFs.getFileBlockLocations(replaceScheme(file, this.replacementScheme, this.underlyingScheme),
        start, len);
  }

  public ContentSummary getContentSummary(Path f) throws java.io.IOException {
    return this.underlyingFs.getContentSummary(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws java.io.IOException {
    this.underlyingFs.renameSnapshot(replaceScheme(path, this.replacementScheme, this.underlyingScheme),
        snapshotOldName, snapshotNewName);
  }

  public void moveFromLocalFile(Path[] srcs, Path dst) throws java.io.IOException {
    this.underlyingFs.moveFromLocalFile(replaceScheme(srcs, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags,
      int bufferSize, short replication, long blockSize, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.createNonRecursive(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        permission, flags, bufferSize, replication, blockSize, progress);
  }

  public boolean rename(Path src, Path dst) throws java.io.IOException {
    return this.underlyingFs.rename(replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream create(Path f) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public boolean mkdirs(Path f) throws java.io.IOException {
    return this.underlyingFs.mkdirs(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        overwrite, bufferSize, replication, blockSize);
  }

  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws java.io.IOException {
    this.underlyingFs.completeLocalOutput(replaceScheme(fsOutputFile, this.replacementScheme, this.underlyingScheme),
        replaceScheme(tmpLocalFile, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataInputStream open(Path f) throws java.io.IOException {
    return this.underlyingFs.open(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        overwrite, bufferSize, progress);
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    this.underlyingFs.setVerifyChecksum(verifyChecksum);
  }

  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
      short replication, long blockSize, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        permission, flags, bufferSize, replication, blockSize, progress);
  }

  public long getDefaultBlockSize() {
    return this.underlyingFs.getDefaultBlockSize();
  }

  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws java.io.IOException {
    this.underlyingFs.copyFromLocalFile(delSrc, overwrite, replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream create(Path f, boolean overwrite) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme), overwrite);
  }

  public long getDefaultBlockSize(Path f) {
    return this.underlyingFs.getDefaultBlockSize(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public boolean exists(Path f) throws java.io.IOException {
    return this.underlyingFs.exists(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public void deleteSnapshot(Path path, String snapshotName) throws java.io.IOException {
    this.underlyingFs.deleteSnapshot(replaceScheme(path, this.replacementScheme, this.underlyingScheme), snapshotName);
  }

  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme), overwrite, bufferSize);
  }

  public void setOwner(Path p, String username, String groupname) throws java.io.IOException {
    this.underlyingFs.setOwner(replaceScheme(p, this.replacementScheme, this.underlyingScheme), username, groupname);
  }

  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws java.io.IOException {
    return this.underlyingFs.listFiles(replaceScheme(f, this.replacementScheme, this.underlyingScheme), recursive);
  }

  public FSDataOutputStream append(Path f, int bufferSize) throws java.io.IOException {
    return this.underlyingFs.append(replaceScheme(f, this.replacementScheme, this.underlyingScheme), bufferSize);
  }

  public boolean deleteOnExit(Path f) throws java.io.IOException {
    return this.underlyingFs.deleteOnExit(replaceScheme(f, this.replacementScheme, this.underlyingScheme));
  }

  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws java.io.IOException {
    return this.underlyingFs.create(replaceScheme(f, this.replacementScheme, this.underlyingScheme),
        permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  public void moveFromLocalFile(Path src, Path dst) throws java.io.IOException {
    this.underlyingFs.moveFromLocalFile(replaceScheme(src, this.replacementScheme, this.underlyingScheme),
        replaceScheme(dst, this.replacementScheme, this.underlyingScheme));
  }

  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws java.io.IOException {
    return replaceScheme(
        this.underlyingFs.globStatus(replaceScheme(pathPattern, this.replacementScheme, this.underlyingScheme), filter),
        this.underlyingScheme, this.replacementScheme);
  }
}
