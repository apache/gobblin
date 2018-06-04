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

package org.apache.gobblin.runtime;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;


/**
 * An object which holds all {@link Throwable}s thrown by {@link org.apache.gobblin.runtime.fork.Fork}, so that other
 * Gobblin components (like {@link Task}) can have access.
 */
@Slf4j
public class ForkThrowableHolder {
  Map<Integer, Throwable> throwables = Maps.newHashMap();

  public void setThrowable(int forkIdx, Throwable e) {
    throwables.put(forkIdx, e);
  }

  public Optional<Throwable> getThrowable (int forkIdx) {
    return Optional.fromNullable(throwables.get(forkIdx));
  }

  public boolean isEmpty() {
    return throwables.isEmpty();
  }

  public ForkException getAggregatedException (List<Integer> failedForkIds, String taskId) {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("Fork branches " + failedForkIds + " failed for task " + taskId + "\n");
    for (Integer idx: failedForkIds) {
      stringBuffer.append("<Fork " + idx + ">\n");
      if (this.throwables.containsKey(idx)) {
        stringBuffer.append(ExceptionUtils.getFullStackTrace(this.throwables.get(idx)));
      } else {
        stringBuffer.append("Cannot find throwable entry in ForkThrowableHolder\n");
      }
    }
    return new ForkException(stringBuffer.toString());
  }
}
