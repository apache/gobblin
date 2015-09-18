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

package gobblin.source.extractor.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Optional;


/**
 * Reads an {@link java.io.InputStream} as an iterator of byte arrays read sequentially from the file.
 */
public class BufferedRawByteIterator implements Iterator<byte[]> {

  private final InputStream is;
  private final byte[] buffer;
  private Optional<byte[]> nextElement;
  private final int bufferSize;

  /**
   * @param is {@link java.io.InputStream} to read.
   * @param bufferSize Desired size of returned byte arrays (no guarantee that all arrays will be of this size).
   */
  public BufferedRawByteIterator(InputStream is, int bufferSize) {
    this.is = is;
    this.bufferSize = bufferSize;
    this.buffer = new byte[bufferSize];
    this.nextElement = Optional.absent();
  }

  @Override public boolean hasNext() {

    if(this.nextElement.isPresent()) {
      return true;
    }

    try {
      int bytes = this.is.read(this.buffer, 0, this.bufferSize);
      if(bytes <= 0) {
        return false;
      }
      this.nextElement = Optional.of(Arrays.copyOf(this.buffer, bytes));
      return true;
    } catch(IOException ioe) {
      return false;
    }
  }

  @Override public byte[] next() {

    if(!hasNext()) {
      throw new NoSuchElementException();
    }

    byte[] toReturn = this.nextElement.get();
    this.nextElement = Optional.absent();

    return toReturn;
  }
}
