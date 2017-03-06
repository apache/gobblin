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
   *
   * @param thing: the object that needs to be copied
   * @return: true if CopyHelper can copy this thing, false otherwise
   */
  public static boolean isCopyable(Object thing) {
    if (
        (thing instanceof Copyable)
        || (thing instanceof byte[])
        || (thing instanceof String)
        || (thing instanceof Integer)
        || (thing instanceof Long)
        ) {
      return true;
    }
    return false;
  }

  /**
   *
   * @param thing: this object that needs to be copied
   * @return: a copied instance
   * @throws CopyNotSupportedException if thing cannot be copied
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

    if (thing instanceof String
        || thing instanceof Integer
        || thing instanceof Long) {
      // These are immutable so don't need to copy
      return thing;
    }
    throw new CopyNotSupportedException(thing.getClass().getName() + " cannot be copied.");
  }

}
