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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.commons.httpclient.HttpStatus;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alpha;
import gobblin.writer.objectstore.response.DeleteResponse;

/**
 * An {@link ObjectStoreOperation} that deletes an object with <code>objectId</code> in the object store.
 */
@Alpha
@AllArgsConstructor(access=AccessLevel.PRIVATE)
@Getter
public class ObjectStoreDeleteOperation extends ObjectStoreOperation<DeleteResponse> {

  /**
   * Id of the object to be deleted
   */
  private final byte[] objectId;
  /**
   * Additional delete configurations if any
   */
  private final Config deleteConfig;

  /**
   * Calls {@link ObjectStoreClient#delete(String, Config)} for the object ot be deleted
   *
   * {@inheritDoc}
   * @see gobblin.writer.objectstore.ObjectStoreOperation#execute(gobblin.writer.objectstore.ObjectStoreClient)
   */
  @Override
  public DeleteResponse execute(ObjectStoreClient objectStoreClient) throws IOException {
    objectStoreClient.delete(this.objectId, this.deleteConfig);
    return new DeleteResponse(HttpStatus.SC_ACCEPTED);
  }

  /**
   * A builder to build new {@link ObjectStoreDeleteOperation}
   */
  public static class Builder {
    private byte[] objectId;
    private Config deleteConfig;

    public Builder withObjectId(byte[] objectId) {
      this.objectId = objectId;
      return this;
    }

    public Builder withDeleteConfig(Config deleteConfig) {
      this.deleteConfig = deleteConfig;
      return this;
    }

    public ObjectStoreDeleteOperation build() {
      Preconditions.checkArgument(this.objectId != null, "Object Id needs to be set");
      if (this.deleteConfig == null) {
        this.deleteConfig = ConfigFactory.empty();
      }
      return new ObjectStoreDeleteOperation(this.objectId, this.deleteConfig);
    }
  }
}
