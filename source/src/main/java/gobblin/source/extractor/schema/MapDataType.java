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

package gobblin.source.extractor.schema;

public class MapDataType extends DataType {
  String values;

  public MapDataType(String type, String values) {
    super(type);
    this.values = values;
  }

  public String getValues() {
    return values;
  }

  public void setValues(String values) {
    this.values = values;
  }
}
