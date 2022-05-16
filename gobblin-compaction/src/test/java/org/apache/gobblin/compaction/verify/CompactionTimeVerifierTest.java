/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.compaction.verify;

import org.testng.Assert;
import org.testng.annotations.Test;


public class CompactionTimeVerifierTest {

  @Test
  public void testOneDatasetTime() {
    String timeString = "Identity.MemberAccount:1d2h";

    String lb1 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMatchedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb2, "2d");

    timeString = "2d;Identity.MemberAccount:1d2h";
    String lb3 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb3, "1d2h");
    String lb4 = CompactionTimeRangeVerifier.getMatchedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb4, "2d");
  }

  @Test
  public void testTwoDatasetTime() {
    String timeString = "Identity.*:1d2h;BizProfile.BizCompany:3d";

    String lb1 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMatchedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb2, "3d");
    String lb3 = CompactionTimeRangeVerifier.getMatchedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb3, "2d");

    timeString = "2d;Identity.MemberAccount:1d2h;BizProfile.BizCompany:3d";
    String lb4 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb4, "1d2h");
    String lb5 = CompactionTimeRangeVerifier.getMatchedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb5, "3d");
    String lb6 = CompactionTimeRangeVerifier.getMatchedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb6, "2d");
  }

  @Test
  public void testDefaultDatasetTime() {
    String timeString = "Identity.*:1d2h;3d2h;BizProfile.BizCompany:3d";
    String lb1 = CompactionTimeRangeVerifier.getMatchedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb1, "3d2h");

    timeString = "3d2h";
    String lb2 = CompactionTimeRangeVerifier.getMatchedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb2, "3d2h");
  }

  @Test
  public void testEmptySpace() {
    String timeString = "Identity.* :   1d2h ; BizProfile.BizCompany : 3d";

    String lb1 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMatchedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb2, "3d");
    String lb3 = CompactionTimeRangeVerifier.getMatchedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb3, "2d");

    timeString = "2d;Identity.MemberAccount  :1d2h;   BizProfile.BizCompany:3d";
    String lb4 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb4, "1d2h");
    String lb5 = CompactionTimeRangeVerifier.getMatchedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb5, "3d");
    String lb6 = CompactionTimeRangeVerifier.getMatchedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb6, "2d");
  }

  @Test
  public void testPartialMatchedNames() {
    String timeString = "Identity.Member$ :   1d2h";
    String lb1 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/Member", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMatchedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb2, "2d");
  }
}
