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
package gobblin.util.callbacks;

import com.google.common.base.Function;

/**
 * Factory that binds in a {@link Runnable}s a callback to specific object to be sent to.
 * @param L   the type of the listener object. This will be passed to the callback runnable.
 * @param R   the type of the result
 */
public interface CallbackFactory<L, R> {
  /** Create a runnable that will execute the callback on the supplied listener */
  Function<L, R> createCallbackRunnable();
}
