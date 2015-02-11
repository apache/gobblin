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

package gobblin.source.extractor.extract.sftp;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.filebased.FileBasedExtractor;


/**
 * Abstract class that implements the SFTP
 * protocol for connecting to source
 * and downloading files
 * @author stakiar
 */
public class SftpExtractor<S, D> extends FileBasedExtractor<S, D> {
  public SftpExtractor(WorkUnitState workUnitState) {
    super(workUnitState, new SftpFsHelper(workUnitState));
  }
}
