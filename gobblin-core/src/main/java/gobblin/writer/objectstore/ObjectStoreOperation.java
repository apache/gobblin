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
package gobblin.writer.objectstore;

import java.io.IOException;

import gobblin.annotation.Alpha;
import gobblin.converter.objectstore.ObjectStoreConverter;


/**
 * An {@link ObjectStoreOperation} is the record type used by {@link ObjectStoreWriter}s and {@link ObjectStoreConverter}.
 * This class represents an operation performed for an object in an object store. The store can be accessed using {@link ObjectStoreClient}.
 * Some of the operations are DELETE, PUT, GET etc.
 * Subclasses are specific operations, they need to implement the {@link #execute(ObjectStoreClient)} method to perform their
 * operation on an object in the store.
 *
 * @param <T> Response type of the operation
 */
@Alpha
public abstract class ObjectStoreOperation<T> {

  /**
   * {@link ObjectStoreWriter} calls this method for every {@link ObjectStoreOperation}. This method should be used by
   * the operation to make necessary calls to object store. The operation can use <code>objectStoreClient</code> to talk
   * to the store
   *
   * @param objectStoreClient a client to the object store
   * @return the response of this operation
   * @throws IOException when the operation fails
   */
  public abstract T execute(ObjectStoreClient objectStoreClient) throws IOException;
}
