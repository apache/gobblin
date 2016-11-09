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

import org.testng.Assert;
import org.testng.annotations.Test;


public class ConsoleWriterTest {

  @Test
  public void testNull()
  {
    ConsoleWriter<String> consoleWriter = new ConsoleWriter<>();
    String foo = null;
    try {
      consoleWriter.write(foo);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on null");
    }
  }

  @Test
  public void testInteger()
  {
    ConsoleWriter<Integer> consoleWriter = new ConsoleWriter<>();
    Integer foo = 1;
    try {
      consoleWriter.write(foo);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on writing an Integer");
    }
  }

  private class TestObject {

  };

  @Test
  public void testObject()
  {

    TestObject testObject = new TestObject();
    ConsoleWriter<TestObject> consoleWriter = new ConsoleWriter<>();
    try {
      consoleWriter.write(testObject);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on writing an object that doesn't explicitly implement toString");
    }

    testObject = null;
    try {
      consoleWriter.write(testObject);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on writing a null object");
    }

  }

}
