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

package gobblin.source.extractor.filebased;

import gobblin.util.Decorator;

import java.io.InputStream;
import java.util.List;

/**
 * A decorator that delegates to inner {@link FileBasedHelper}.
 * {@link #getFileSize(String)} is not implemented by this class.
 */
public class SizeAwareFileBasedHelperDecorator implements SizeAwareFileBasedHelper, Decorator {

  private final FileBasedHelper fileBasedHelper;

  public SizeAwareFileBasedHelperDecorator(FileBasedHelper fileBasedHelper) {
    this.fileBasedHelper = fileBasedHelper;
  }

  @Override
  public void connect() throws FileBasedHelperException {
    fileBasedHelper.connect();

  }

  @Override
  public void close() throws FileBasedHelperException {
    fileBasedHelper.close();

  }

  @Override
  public List<String> ls(String path) throws FileBasedHelperException {
    return fileBasedHelper.ls(path);
  }

  @Override
  public InputStream getFileStream(String path) throws FileBasedHelperException {
    return fileBasedHelper.getFileStream(path);
  }

  @Override
  public long getFileSize(String path) throws FileBasedHelperException {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public Object getDecoratedObject() {
    return fileBasedHelper;
  }
}
