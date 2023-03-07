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

package org.apache.gobblin.data.management.copy;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.api.client.util.Lists;


/**
 * Wrapper for owner, group, permission including sticky bit and ACL entry of a path.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OwnerAndPermission implements Writable {

  private String owner;
  private String group;
  private FsPermission fsPermission;
  private List<AclEntry> aclEntries;

  public OwnerAndPermission (String owner, String group, FsPermission fsPermission) {
    this(owner, group, fsPermission, Lists.newArrayList());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Text.writeString(dataOutput, this.owner);
    Text.writeString(dataOutput, this.group);
    this.fsPermission.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.owner = Text.readString(dataInput);
    this.group = Text.readString(dataInput);
    this.fsPermission = FsPermission.read(dataInput);
  }

  /**
   * Read a {@link org.apache.gobblin.data.management.copy.OwnerAndPermission} from a {@link java.io.DataInput}.
   * @throws IOException
   */
  public static OwnerAndPermission read(DataInput input) throws IOException {
    OwnerAndPermission oap = new OwnerAndPermission();
    oap.readFields(input);
    return oap;
  }

  /**
   * given a file, return whether the metadata for the file match the current owner and permission
   * note: if field is null, we always think it's match as no update needed.
   * @param file the file status that need to be evaluated
   * @return true if the metadata for the file match the current owner and permission
   */
  public boolean hasSameOwnerAndPermission(FileSystem fs, FileStatus file) throws IOException {
    return this.hasSameFSPermission(file) && this.hasSameGroup(file) && this.hasSameOwner(file) && this.hasSameAcls(fs.getAclStatus(file.getPath()).getEntries());
  }

  private boolean hasSameGroup(FileStatus file) {
    return this.group == null || file.getGroup().equals(this.group);
  }

  private boolean hasSameOwner(FileStatus file) {
    return this.owner == null || file.getOwner().equals(this.owner);
  }

  private boolean hasSameFSPermission(FileStatus file) {
    return this.fsPermission == null || file.getPermission().equals(this.fsPermission);
  }

  private boolean hasSameAcls(List<AclEntry> acls) {
    return this.aclEntries.isEmpty() || acls.equals(aclEntries);
  }
}
