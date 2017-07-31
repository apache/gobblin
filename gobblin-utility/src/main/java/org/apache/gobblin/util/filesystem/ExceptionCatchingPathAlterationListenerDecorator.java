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

package org.apache.gobblin.util.filesystem;

import org.apache.hadoop.fs.Path;

import org.apache.gobblin.util.Decorator;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A decorator for {@link PathAlterationListener} that catches and logs any exception thrown by the underlying listener,
 * preventing it from failing the application.
 */
@AllArgsConstructor
@Slf4j
public class ExceptionCatchingPathAlterationListenerDecorator implements PathAlterationListener, Decorator {

  private final PathAlterationListener underlying;

  @Override
  public Object getDecoratedObject() {
    return this.underlying;
  }

  @Override
  public void onStart(PathAlterationObserver observer) {
    try {
      this.underlying.onStart(observer);
    } catch (Throwable exc) {
      log.error("onStart failure: ", exc);
    }
  }

  @Override
  public void onFileCreate(Path path) {
    try {
      this.underlying.onFileCreate(path);
    } catch (Throwable exc) {
      log.error("onFileCreate failure: ", exc);
    }
  }

  @Override
  public void onFileChange(Path path) {
    try {
      this.underlying.onFileChange(path);
    } catch (Throwable exc) {
      log.error("onFileChange failure: ", exc);
    }
  }

  @Override
  public void onStop(PathAlterationObserver observer) {
    try {
      this.underlying.onStop(observer);
    } catch (Throwable exc) {
      log.error("onStop failure: ", exc);
    }
  }

  @Override
  public void onDirectoryCreate(Path directory) {
    try {
      this.underlying.onDirectoryCreate(directory);
    } catch (Throwable exc) {
      log.error("onDirectoryCreate failure: ", exc);
    }
  }

  @Override
  public void onDirectoryChange(Path directory) {
    try {
      this.underlying.onDirectoryChange(directory);
    } catch (Throwable exc) {
      log.error("onDirectoryChange failure: ", exc);
    }
  }

  @Override
  public void onDirectoryDelete(Path directory) {
    try {
      this.underlying.onDirectoryDelete(directory);
    } catch (Throwable exc) {
      log.error("onDirectoryDelete failure: ", exc);
    }
  }

  @Override
  public void onFileDelete(Path path) {
    try {
      this.underlying.onFileDelete(path);
    } catch (Throwable exc) {
      log.error("onFileDelete failure: ", exc);
    }
  }
}
