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

package gobblin.source.extractor.filebased;

import java.io.InputStream;
import java.util.List;


public interface FileBasedHelper {
  public void connect()
      throws FileBasedHelperException;

  public void close()
      throws FileBasedHelperException;

  public List<String> ls(String path)
      throws FileBasedHelperException;

  public InputStream getFileStream(String path)
      throws FileBasedHelperException;
}
