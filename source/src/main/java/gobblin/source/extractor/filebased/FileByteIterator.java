/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.filebased;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Throwables;


public class FileByteIterator implements Iterator<Byte> {

  private BufferedInputStream bufferedInputStream;

  public FileByteIterator(InputStream inputStream)
      throws IOException {
    this.bufferedInputStream = new BufferedInputStream(inputStream);
  }

  @Override
  public boolean hasNext() {
    try {
      return bufferedInputStream.available() > 0;
    } catch (IOException e) {
      Throwables.propagate(e);
      return false;
    }
  }

  @Override
  public Byte next() {
    try {
      if (this.hasNext()) {
        return new Byte((byte) bufferedInputStream.read());
      } else {
        throw new NoSuchElementException("No more data left in the file");
      }
    } catch (IOException e) {
      Throwables.propagate(e);
      return null;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}