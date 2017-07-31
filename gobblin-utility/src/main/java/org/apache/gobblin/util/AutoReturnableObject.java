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

package org.apache.gobblin.util;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.pool2.impl.GenericObjectPool;


/**
 * Borrow an object from a {@link GenericObjectPool} and returns it automatically on close. Useful for try with resource.
 */
public class AutoReturnableObject<T> implements Closeable {

  private final T object;
  private final GenericObjectPool<T> pool;
  private boolean returned;

  public AutoReturnableObject(GenericObjectPool<T> pool) throws IOException {
    try {
      this.pool = pool;
      this.object = pool.borrowObject();
      this.returned = false;
    } catch (Exception exc) {
      throw new IOException(exc);
    }
  }

  /**
   * @return the object borrowed from {@link GenericObjectPool}.
   * @throws IOException
   */
  public T get() throws IOException {
    if (this.returned) {
      throw new IOException(this.getClass().getCanonicalName() + " has already been closed.");
    }
    return this.object;
  }

  /**
   * Return the borrowed object to the pool.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    try {
      this.pool.returnObject(this.object);
    } catch (Exception exc) {
      throw new IOException(exc);
    } finally {
      this.returned = true;
    }
  }
}
