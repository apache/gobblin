/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.tunnel;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A convenient, trackable, easy-to-cleanup wrapper around threads.
 *
 * @author kkandekar@linkedin.com
 */
abstract class EasyThread extends Thread {
  static Set<EasyThread> ALL_THREADS = Collections.synchronizedSet(new HashSet<EasyThread>());

  EasyThread startThread() {
    setDaemon(true);
    start();
    ALL_THREADS.add(this);
    return this;
  }

  @Override
  public void run() {
    try {
      runQuietly();
    } catch (Exception ignored) {
    }
  }

  abstract void runQuietly() throws Exception;
}
