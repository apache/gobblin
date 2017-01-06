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
package gobblin.writer.objectstore;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import com.typesafe.config.Config;

import gobblin.annotation.Alpha;
import gobblin.writer.objectstore.response.GetObjectResponse;

/**
 * A client interface to interact with an object store. Supports basic operations like put,delete and get.
 */
@Alpha
public interface ObjectStoreClient {

  /**
   * Puts an object in <code>objectStream</code> to the store. Returns the id of the object created.
   *
   * @param objectStream to put in the store
   * @param putConfig additional config if any (User metadata, put options etc.)
   * @return the id of newly created object
   * @throws IOException if put failed
   */
  public byte[] put(InputStream objectStream, Config putConfig) throws IOException;

  /**
   * Puts an object in <code>objectStream</code> to the store at <code>objectId</code>.
   * Returns the id of the object created.
   *
   * @param objectStream to put in the store
   * @param objectId to put in the store
   * @param putConfig additional config if any (User metadata, put options etc.)
   * @return the id of newly created object
   * @throws IOException if put failed
   */
  public byte[] put(InputStream objectStream, @Nonnull byte[] objectId, Config putConfig) throws IOException;

  /**
   * Delete an object with <code>objectId</code> in the store. Operation is a noop if object does not exist
   *
   * @param objectId to delete
   * @param deleteConfig additional config if any
   * @throws IOException if delete failed
   */
  public void delete(@Nonnull byte[] objectId, Config deleteConfig) throws IOException;

  /**
   * Get metadata associated with an object
   * @param objectId for which metadata is retrieved
   * @return object metadata
   * @throws IOException if get object metadata fails
   */
  public Config getObjectProps(@Nonnull byte[] objectId) throws IOException;

  /**
   * Set metadata associated with an object
   * @param objectId for which metadata is set
   * @param objectProps to set for this object
   * @throws IOException if setting metadata fails
   */
  public void setObjectProps(@Nonnull byte[] objectId, Config objectProps) throws IOException;

  /**
   * Get an object with id <code>objectId</code> stored in the store.
   * @param objectId to retrieve
   * @return a {@link GetObjectResponse} with an {@link InputStream} to the object and object metadata
   * @throws IOException if object does not exist or there was a failure reading the object
   */
  public GetObjectResponse getObject(@Nonnull byte[] objectId) throws IOException;

  /**
   * Close the client
   * @throws IOException
   */
  public void close() throws IOException;
}
