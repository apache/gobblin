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

package org.apache.gobblin.util.guid;

import lombok.EqualsAndHashCode;

import java.io.IOException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.google.common.base.Charsets;


/**
 * Class wrapping a byte array representing a guid. A {@link Guid} is intended to uniquely identify objects in a
 * replicable way.
 */
@EqualsAndHashCode
public class Guid {

  public static final int GUID_LENGTH = 20;

  final byte[] sha;

  /**
   * Creates a {@link Guid} by computing the sha of the input bytes.
   */
  public Guid(byte[] bytes) {
    this(bytes, false);
  }

  /**
   * @param bytes byte array.
   * @param isSha true if bytes are already a sha.
   */
  private Guid(byte[] bytes, boolean isSha) {
    if (isSha) {
      this.sha = bytes;
    } else {
      this.sha = computeGuid(bytes);
    }
  }

  /**
   * Generate a {@link Guid} for an array of {@link HasGuid}.
   * @param objs array of {@link HasGuid}.
   * @return a single {@link Guid} for the array.
   * @throws IOException
   */
  public static Guid fromHasGuid(HasGuid... objs) throws IOException {
    byte[][] byteArrays = new byte[objs.length][];
    for (int i = 0; i < objs.length; i++) {
      byteArrays[i] = objs[i].guid().sha;
    }
    return fromByteArrays(byteArrays);
  }

  /**
   * Generate a {@link Guid} for an array of Strings.
   * @param strings array of Strings.
   * @return a single {@link Guid} for the array.
   * @throws IOException
   */
  public static Guid fromStrings(String... strings) throws IOException {

    if (strings == null || strings.length == 0) {
      throw new IOException("Attempting to compute guid for an empty array.");
    }

    return new Guid(StringUtils.join(strings).getBytes(Charsets.UTF_8));
  }

  /**
   * Generate a {@link Guid} for an array of byte arrays.
   * @param byteArrays array of byte arrays.
   * @return a single {@link Guid} for the array.
   * @throws IOException
   */
  public static Guid fromByteArrays(byte[]... byteArrays) throws IOException {
    if (byteArrays == null || byteArrays.length == 0) {
      throw new IOException("Attempting to compute guid for an empty array.");
    }

    if (byteArrays.length == 1) {
      return new Guid(byteArrays[0]);
    }
    byte[] tmp = new byte[0];
    for (byte[] arr : byteArrays) {
      tmp = ArrayUtils.addAll(tmp, arr);
    }
    return new Guid(tmp);
  }

  /**
   * Reverse of {@link #toString}. Deserializes a {@link Guid} from a previously serialized one.
   * @param str Serialized {@link Guid}.
   * @return deserialized {@link Guid}.
   * @throws IOException
   */
  public static Guid deserialize(String str) throws IOException {
    if (str.length() != 2 * GUID_LENGTH) {
      throw new IOException("String is not an encoded guid.");
    }
    try {
      return new Guid(Hex.decodeHex(str.toCharArray()), true);
    } catch (DecoderException de) {
      throw new IOException(de);
    }
  }

  /**
   * Combine multiple {@link Guid}s into a single {@link Guid}.
   * @throws IOException
   */
  public static Guid combine(Guid... guids) throws IOException {
    byte[][] byteArrays = new byte[guids.length][];
    for (int i = 0; i < guids.length; i++) {
      byteArrays[i] = guids[i].sha;
    }
    return fromByteArrays(byteArrays);
  }

  /**
   * Creates a new {@link Guid} which is a unique, replicable representation of the pair (this, byteArrays).
   * @param byteArrays an array of byte arrays.
   * @return a new {@link Guid}.
   * @throws IOException
   */
  public Guid append(byte[]... byteArrays) throws IOException {
    if (byteArrays == null || byteArrays.length == 0) {
      return this;
    }
    return fromByteArrays(ArrayUtils.add(byteArrays, this.sha));
  }

  /**
   * Creates a new {@link Guid} which is a unique, replicable representation of the pair (this, guids). Equivalent to
   * combine(this, guid1, guid2, ...)
   * @param guids an array of {@link Guid}.
   * @return a new {@link Guid}.
   * @throws IOException
   */
  public Guid append(Guid... guids) throws IOException {
    if (guids == null || guids.length == 0) {
      return this;
    }
    return combine(ArrayUtils.add(guids, this));
  }

  /**
   * Creates a new {@link Guid} which is a unique, replicable representation of the pair (this, objs).
   * @param objs an array of {@link HasGuid}.
   * @return a new {@link Guid}.
   * @throws IOException
   */
  public Guid append(HasGuid... objs) throws IOException {
    if (objs == null || objs.length == 0) {
      return this;
    }
    return fromHasGuid(ArrayUtils.add(objs, new SimpleHasGuid(this)));
  }

  /**
   * Serializes the guid into a hex string. The original {@link Guid} can be recovered using {@link #deserialize}.
   */
  @Override
  public String toString() {
    return Hex.encodeHexString(this.sha);
  }

  // DigestUtils.sha is deprecated for sha1, but sha1 is not available in old versions of commons codec
  @SuppressWarnings("deprecation")
  private static byte[] computeGuid(byte[] bytes) {
    return DigestUtils.sha(bytes);
  }

  static class SimpleHasGuid implements HasGuid {
    private final Guid guid;

    public SimpleHasGuid(Guid guid) {
      this.guid = guid;
    }

    @Override
    public Guid guid() throws IOException {
      return this.guid;
    }
  }

}
