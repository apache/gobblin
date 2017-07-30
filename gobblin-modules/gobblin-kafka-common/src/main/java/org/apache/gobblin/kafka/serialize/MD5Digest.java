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

package gobblin.kafka.serialize;

import java.util.Arrays;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.google.common.base.Preconditions;


/**
 * A holder for an MD5Digest
 * Allows for conversion between the human-readable String version and the serializable byte[] version.
 * Used by the {@link gobblin.kafka.schemareg.LiKafkaSchemaRegistry}
 */
public class MD5Digest {

  public static final int MD5_BYTES_LENGTH = 16;
  private final byte[] bytes;
  private final String md5String;

  private MD5Digest(String md5String, byte[] md5bytes) {
    this.bytes = md5bytes;
    this.md5String = md5String;
  }

  public String asString() {
    return this.md5String;
  }

  public byte[] asBytes() {
    return this.bytes;
  }

  /**
   * Static method to get an MD5Digest from a human-readable string representation
   * @param md5String
   * @return a filled out MD5Digest
   */
  public static MD5Digest fromString(String md5String) {
    byte[] bytes;
    try {
      bytes = Hex.decodeHex(md5String.toCharArray());
      return new MD5Digest(md5String, bytes);
    } catch (DecoderException e) {
      throw new IllegalArgumentException("Unable to convert md5string", e);
    }
  }

  /**
   * Static method to get an MD5Digest from a binary byte representation
   * @param md5Bytes
   * @return a filled out MD5Digest
   */
  public static MD5Digest fromBytes(byte[] md5Bytes)
  {
    Preconditions.checkArgument(md5Bytes.length == MD5_BYTES_LENGTH,
        "md5 bytes must be " + MD5_BYTES_LENGTH + " bytes in length, found " + md5Bytes.length + " bytes.");
    String md5String = Hex.encodeHexString(md5Bytes);
    return new MD5Digest(md5String, md5Bytes);
  }

  /**
   * Static method to get an MD5Digest from a binary byte representation.
   * @param md5Bytes
   * @param offset in the byte array to start reading from
   * @return a filled out MD5Digest
   */
  public static MD5Digest fromBytes(byte[] md5Bytes, int offset) {
    byte[] md5BytesCopy = Arrays.copyOfRange(md5Bytes, offset, offset + MD5_BYTES_LENGTH);
    //TODO: Replace this with a version that encodes without needing a copy.
    String md5String = Hex.encodeHexString(md5BytesCopy);
    return new MD5Digest(md5String, md5BytesCopy);
  }

  @Override
  public int hashCode() {
    //skipping null check since there is no way to create MD5Digest with a null md5String
    return this.md5String.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof MD5Digest)) return false;

    MD5Digest other = (MD5Digest) obj;
    if (this.md5String == null)
    {
      return (other.md5String == null);
    }
    else
    {
      return this.md5String.equals(other.md5String);
    }
  }
}

