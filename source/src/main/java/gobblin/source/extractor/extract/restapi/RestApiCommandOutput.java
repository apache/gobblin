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

package gobblin.source.extractor.extract.restapi;

import java.util.HashMap;
import java.util.Map;

import gobblin.source.extractor.extract.CommandOutput;


public class RestApiCommandOutput implements CommandOutput<RestApiCommand, String> {

  private Map<RestApiCommand, String> results = new HashMap<RestApiCommand, String>();

  @Override
  public void storeResults(Map<RestApiCommand, String> results) {
    this.results = results;
  }

  @Override
  public Map<RestApiCommand, String> getResults() {
    return results;
  }

  @Override
  public void put(RestApiCommand key, String value) {
    results.put(key, value);
  }
}
