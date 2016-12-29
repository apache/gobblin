/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.writer;

/**
 * A generic write response to wrap responses from other systems.
 * Provides default values for things expected from write responses.
 * @param <T>
 */
public class GenericWriteResponse<T> implements WriteResponse<T> {

  private final T _innerResponse;

  public GenericWriteResponse(T innerResponse)
  {
    _innerResponse = innerResponse;
  }

  @Override
  public T getRawResponse() {
    return _innerResponse;
  }

  @Override
  public String getStringResponse() {
    return _innerResponse.toString();
  }

  @Override
  public long bytesWritten() {
    return -1;
  }
}
