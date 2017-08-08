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

package org.apache.gobblin.util.concurrent;

import java.util.concurrent.TimeUnit;


public class AutoResetEvent {
    private final Object syncObject = new Object();
    private boolean state;

    public AutoResetEvent() {
        this(false);
    }

    public AutoResetEvent(boolean initialState) {
        this.state = initialState;
    }

    public void waitOne() throws InterruptedException {
        synchronized (this.syncObject) {
            while (!this.state) {
                this.syncObject.wait();
            }
            this.state = false;
        }
    }

    public boolean waitOne(int timeout, TimeUnit timeUnit) throws InterruptedException {
        long waitTime = timeUnit.toNanos(timeout);
        long startTime = System.nanoTime();
        synchronized (this.syncObject) {
            while (!this.state) {
                long remainingTimeoutNs = waitTime - (System.nanoTime() - startTime);
                if (remainingTimeoutNs <= 0) {
                    return false;
                }
                long remainingTimeoutMs = 0;
                if (remainingTimeoutNs > 999999) {
                    remainingTimeoutMs = remainingTimeoutNs / 1000000;
                    remainingTimeoutNs = remainingTimeoutNs % 1000000;
                }
                this.syncObject.wait(remainingTimeoutMs, (int) remainingTimeoutNs);
            }
            this.state = false;
            return true;
        }
    }

    public void set() {
        synchronized (this.syncObject) {
            this.state = true;
            this.syncObject.notify();
        }
    }
}
