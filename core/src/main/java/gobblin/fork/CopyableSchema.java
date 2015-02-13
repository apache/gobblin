/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.fork;

import org.apache.avro.Schema;


/**
 * A wrapper class for {@link org.apache.avro.Schema} that is also {@link Copyable}.
 *
 * @author ynli
 */
public class CopyableSchema implements Copyable<Schema> {

  private final Schema schema;

  public CopyableSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Schema copy()
      throws CopyNotSupportedException {
    return new Schema.Parser().setValidate(false).parse(this.schema.toString());
  }
}
