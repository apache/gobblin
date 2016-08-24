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

import static org.mockito.Mockito.*;

import java.io.IOException;

import gobblin.configuration.State;
import gobblin.writer.exception.NonTransientException;

import org.junit.Assert;
import org.testng.annotations.Test;

@Test(groups = { "gobblin.writer" })
public class RetryWriterTest {

  public void retryTest() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);
    doThrow(new RuntimeException()).when(writer).write(null);

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, new State());
    DataWriter<Void> retryWriter = builder.build();
    try {
      retryWriter.write(null);
      Assert.fail("Should have failed.");
    } catch (Exception e) { }

    verify(writer, times(5)).write(null);
  }

  public void retryTestNonTransientException() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);
    doThrow(new NonTransientException()).when(writer).write(null);

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, new State());
    DataWriter<Void> retryWriter = builder.build();
    try {
      retryWriter.write(null);
      Assert.fail("Should have failed.");
    } catch (Exception e) { }

    verify(writer, atMost(1)).write(null);
  }

  public void retryTestSuccess() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, new State());
    DataWriter<Void> retryWriter = builder.build();
    retryWriter.write(null);

    verify(writer, times(1)).write(null);
  }
}