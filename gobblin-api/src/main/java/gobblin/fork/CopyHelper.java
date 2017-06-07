/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.fork;

/**
 * A helper class to copy things that may or may not be {@link Copyable}.
 * Supports implementations for common primitive types.
 */
public class CopyHelper {

  /**
   * Check if an object is copyable using the {@link #copy(Object)} method.
   * @param thing: the object that needs to be copied
   * @return: true if {@link CopyHelper} can copy this thing, false otherwise
   */
  public static boolean isCopyable(Object thing) {
    if (
        (thing instanceof Copyable)
            || (thing instanceof byte[])
            || (isImmutableType(thing))
        ) {
      return true;
    }
    return false;
  }

  /**
   * Contains a collection of supported immutable types for copying.
   * Only keep the types that are worth supporting as record types.
   * @param thing: an Object being checked
   * @return true if supported immutable type, false otherwise
   */
  private static boolean isImmutableType(Object thing) {
    return ((thing == null)
        || (thing instanceof String)
        || (thing instanceof Integer)
        || (thing instanceof Long));
  }

  /**
   * Copy this object if needed.
   * @param thing : this object that needs to be copied
   * @return: a possibly copied instance
   * @throws CopyNotSupportedException if thing needs to be copied but cannot be
   */
  public static Object copy(Object thing) throws CopyNotSupportedException {
    if (!isCopyable(thing)) {
      throw new CopyNotSupportedException(thing.getClass().getName() + " cannot be copied. See Copyable");
    }
    if (thing instanceof Copyable) {
      return ((Copyable) thing).copy();
    }
    // Support for a few primitive types out of the box
    if (thing instanceof byte[]) {
      byte[] copy = new byte[((byte[]) thing).length];
      System.arraycopy(thing, 0, copy, 0, ((byte[]) thing).length);
      return copy;
    }

    // Assume that everything other type is immutable, not checking this again
    return thing;
  }

}
