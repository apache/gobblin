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

package com.linkedin.uif.qualitychecker.row;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;


/**
 * A writer to store records that don't pass
 * the a row level policy check
 *
 * @author stakiar
 */
public class RowLevelErrFileWriter {
  private BufferedWriter writer;

  /**
   * Open a BufferedWriter
   * @param errFilePath path to write the file
   */
  public void open(Path errFilePath)
      throws IOException {
    this.writer = new BufferedWriter(new FileWriter(errFilePath.toString()));
  }

  /**
   * Write the string representation of the record
   * @param record the record to write
   */
  public void write(Object record)
      throws IOException {
    this.writer.write(record.toString());
  }

  /**
   * Close the writer
   */
  public void close()
      throws IOException {
    this.writer.close();
  }
}