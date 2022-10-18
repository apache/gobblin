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

import java.util.Arrays;
import java.util.concurrent.Callable;

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
    logSwallowedThrowable(() -> {
      this.underlying.onStart(observer);
      return null;
    });
  }

  @Override
  public void onFileCreate(Path path) {
    logSwallowedThrowable(() -> {
      this.underlying.onFileCreate(path);
      return null;
    });
  }

  @Override
  public void onFileChange(Path path) {
    logSwallowedThrowable(() -> {
      this.underlying.onFileChange(path);
      return null;
    });
  }

  @Override
  public void onStop(PathAlterationObserver observer) {
    logSwallowedThrowable(() -> {
      this.underlying.onStop(observer);
      return null;
    });
  }

  @Override
  public void onDirectoryCreate(Path directory) {
    logSwallowedThrowable(() -> {
      this.underlying.onDirectoryCreate(directory);
      return null;
    });
  }

  @Override
  public void onDirectoryChange(Path directory) {
    logSwallowedThrowable(() -> {
      this.underlying.onDirectoryChange(directory);
      return null;
    });
  }

  @Override
  public void onDirectoryDelete(Path directory) {
    logSwallowedThrowable(() -> {
      this.underlying.onDirectoryDelete(directory);
      return null;
    });
  }

  @Override
  public void onFileDelete(Path path) {
    logSwallowedThrowable(() -> {
      this.underlying.onFileDelete(path);
      return null;
    });
  }

  @Override
  public void onCheckDetectedChange() {
    logSwallowedThrowable(() -> {
      this.underlying.onCheckDetectedChange();
      return null;
    });
  }

  protected void logSwallowedThrowable(Callable<Void> c) {
    try {
      c.call();
    } catch (Throwable exc) {
      String methodName = Arrays.stream(exc.getStackTrace()).findFirst().get().getMethodName();
      log.error(methodName + " failed due to exception:", exc);
    }
  }

}
