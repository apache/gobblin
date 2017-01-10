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
 * A class for encapsulating the system-native response and general statistics from a write
 */
public interface WriteResponse<T> {

  /**
   * Get the raw response returned by the underlying write system
   */
  T getRawResponse();

  /**
   * Get a String representation of the response.
   */
  String getStringResponse();

  /**
   * The number of bytes written as part of this write.
   * @return The number of bytes written as part of this write.
   * -1 if this value is unknown. 0 if nothing was written.
   */
  long bytesWritten();

  WriteResponse EMPTY = new WriteResponse() {
    private final String _emptyResponse = "EmptyResponse";

    @Override
    public Object getRawResponse() {
      return this._emptyResponse;
    }

    @Override
    public String getStringResponse() {
      return this._emptyResponse;
    }

    @Override
    public long bytesWritten() {
      return -1;
    }
  };
}
