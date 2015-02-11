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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


/**
 * A wrapper class for {@link org.apache.avro.generic.GenericRecord}
 * that is also {@link Copyable}.
 *
 * @author ynli
 */
public class CopyableGenericRecord implements Copyable<GenericRecord> {

  private final GenericRecord record;

  public CopyableGenericRecord(GenericRecord record) {
    this.record = record;
  }

  @Override
  public GenericRecord copy()
      throws CopyNotSupportedException {
    if (!(this.record instanceof GenericData.Record)) {
      throw new CopyNotSupportedException(
          "The record to make copy is not an instance of " + GenericData.Record.class.getName());
    }
    // Make a deep copy of the original record
    return new GenericData.Record((GenericData.Record) this.record, true);
  }
}
