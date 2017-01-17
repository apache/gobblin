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

package gobblin.couchbase.common;

import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.document.AbstractDocument;


/**
 * A document type to store raw binary data in Couchbase
 */
public class TupleDocument extends AbstractDocument<Tuple2<ByteBuf, Integer>>
{
  public TupleDocument(String id, Tuple2<ByteBuf, Integer> content)
  {
    this(id, 0, content, 0);
  }

  public TupleDocument(String id, int expiry, Tuple2<ByteBuf, Integer> content, long cas)
  {
    super(id, expiry, content, cas);
  }

  public TupleDocument(String id, int expiry, Tuple2<ByteBuf, Integer> content, long cas, MutationToken mutationToken)
  {
    super(id, expiry, content, cas, mutationToken);
  }

}



