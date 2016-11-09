/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import lombok.extern.slf4j.Slf4j;


/**
 * A simple console writer that prints the record to stdout
 */
@Slf4j
public class ConsoleWriter<D> implements DataWriter<D> {
  private long _recordsWritten = 0;
  @Override
  public void write(D record)
      throws IOException {
    System.out.println(record);
    if (record != null)
    {
      log.info(record.toString());
    }
    else
    {
      log.info("null record");
    }
    ++_recordsWritten;
  }

  @Override
  public void commit()
      throws IOException {
    log.debug("Commit called.");

  }

  @Override
  public void cleanup()
      throws IOException {
    log.debug("Cleanup called.");
  }

  @Override
  public long recordsWritten() {
    return _recordsWritten;
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return 0;
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
  }
}

