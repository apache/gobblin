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

package gobblin.data.management.copy;

import gobblin.data.management.partition.File;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;


/**
 * Abstraction for a file to copy from {@link #origin} to {@link #destination}.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class CopyableFile implements File, Writable {

  public static final String SERIALIZED_COPYABLE_FILE = "gobblin.copy.serialized.copyable.file";

  /** {@link FileStatus} of the existing origin file. */
  private FileStatus origin;
  /** Destination {@link Path} of the file. */
  private Path destination;
  /** Desired {@link OwnerAndPermission} of the destination path. */
  private OwnerAndPermission destinationOwnerAndPermission;
  /**
   * Desired {@link OwnerAndPermission} of the ancestor directories of the destination path. The list is ordered from
   * deepest to highest directory.
   *
   * <p>
   * For example, if {@link #destination} is /a/b/c/file, then the first element of this list is the desired owner and
   * permission for directory /a/b/c, the second is the desired owner and permission for directory /a/b, and so on.
   * </p>
   *
   * <p>
   * If there are fewer elements in the list than ancestor directories in {@link #destination}, it is understood that
   * extra directories are allowed to have any owner and permission.
   * </p>
   */
  private List<OwnerAndPermission> ancestorsOwnerAndPermission;
  /** Checksum of the origin file. */
  private byte[] checksum;

  @Override
  public FileStatus getFileStatus() {
    return this.origin;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    this.origin.write(dataOutput);
    Text.writeString(dataOutput, this.destination.toString());
    this.destinationOwnerAndPermission.write(dataOutput);
    dataOutput.writeInt(this.ancestorsOwnerAndPermission.size());
    for (OwnerAndPermission oap : this.ancestorsOwnerAndPermission) {
      oap.write(dataOutput);
    }
    dataOutput.writeInt(this.checksum.length);
    dataOutput.write(this.checksum);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.origin = new FileStatus();
    this.origin.readFields(dataInput);
    this.destination = new Path(Text.readString(dataInput));
    this.destinationOwnerAndPermission = OwnerAndPermission.read(dataInput);
    int ancestors = dataInput.readInt();
    this.ancestorsOwnerAndPermission = Lists.newArrayList();
    for (int i = 0; i < ancestors; i++) {
      this.ancestorsOwnerAndPermission.add(OwnerAndPermission.read(dataInput));
    }
    int checksumSize = dataInput.readInt();
    this.checksum = new byte[checksumSize];
    dataInput.readFully(this.checksum);
  }

  /**
   * Read a {@link gobblin.data.management.copy.CopyableFile} from a {@link java.io.DataInput}.
   *
   * @throws IOException
   */
  public static CopyableFile read(DataInput dataInput) throws IOException {
    CopyableFile copyableFile = new CopyableFile();
    copyableFile.readFields(dataInput);
    return copyableFile;
  }

  /**
   * Serialize an instance of {@link CopyableFile} into a {@link String}. Usually use to store the copyableFile in state
   * at key {@link #SERIALIZED_COPYABLE_FILE}
   *
   * @param copyableFile to be serialized
   * @return serialized string
   */
  public static String serializeCopyableFile(CopyableFile copyableFile) throws IOException {

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    copyableFile.write(new DataOutputStream(os));
    String toReturn = Hex.encodeHexString(os.toByteArray());
    os.close();

    return toReturn;
  }

  /**
   * Deserializes the string at {@link #SERIALIZED_COPYABLE_FILE} in the passed in {@link Properties}.
   *
   * @param props that contains serialized {@link CopyableFile} at {@link #SERIALIZED_COPYABLE_FILE}
   * @return a new instance of {@link CopyableFile}
   */
  public static CopyableFile deserializeCopyableFile(Properties props) throws IOException {

    try {
      String string = props.getProperty(SERIALIZED_COPYABLE_FILE);
      byte[] actualBytes = Hex.decodeHex(string.toCharArray());
      ByteArrayInputStream is = new ByteArrayInputStream(actualBytes);
      CopyableFile copyableFile = CopyableFile.read(new DataInputStream(is));
      is.close();

      return copyableFile;
    } catch (DecoderException de) {
      throw new IOException(de);
    }
  }

}
