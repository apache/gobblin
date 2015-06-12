/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.converter.s3;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;

/**
 * Tests for {@link ELBToProtobufConverter}
 *
 * @author ahollenbach@nerdwallet.com
 */
@Test(groups = {"gobblin.converter.s3"})
public class ELBToProtobufConverterTest {

  /**
   * A simple use case test.
   */
  @Test
  public void testParseRequestStringSimple() {
    SimpleDateFormat df = new SimpleDateFormat(ELBToProtobufConverter.ISO8601_DATE_FORMAT);

    Assert.assertNotEquals(ELBToProtobufConverter.parseDate("2015-06-08T21:54:25.889826Z", df), null);
    Assert.assertNotEquals(ELBToProtobufConverter.parseDate("2015-06-10T13:55:44.504862Z", df), null);
    Assert.assertNotEquals(ELBToProtobufConverter.parseDate("2015-10-08T01:08:04.997527Z", df), null);
  }
}
