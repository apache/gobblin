package org.apache.gobblin.compaction.verify;

import org.testng.Assert;
import org.testng.annotations.Test;


public class CompactionTimeVerifierTest {

  @Test
  public void testOneDatasetTime() {
    String timeString = "Identity.MemberAccount:1d2h";

    String lb1 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMachedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb2, "2d");

    timeString = "2d;Identity.MemberAccount:1d2h";
    String lb3 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb3, "1d2h");
    String lb4 = CompactionTimeRangeVerifier.getMachedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb4, "2d");
  }

  @Test
  public void testTwoDatasetTime() {
    String timeString = "Identity.*:1d2h;BizProfile.BizCompany:3d";

    String lb1 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMachedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb2, "3d");
    String lb3 = CompactionTimeRangeVerifier.getMachedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb3, "2d");

    timeString = "2d;Identity.MemberAccount:1d2h;BizProfile.BizCompany:3d";
    String lb4 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb4, "1d2h");
    String lb5 = CompactionTimeRangeVerifier.getMachedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb5, "3d");
    String lb6 = CompactionTimeRangeVerifier.getMachedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb6, "2d");
  }

  @Test
  public void testDefaultDatasetTime() {
    String timeString = "Identity.*:1d2h;3d2h;BizProfile.BizCompany:3d";
    String lb1 = CompactionTimeRangeVerifier.getMachedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb1, "3d2h");

    timeString = "3d2h";
    String lb2 = CompactionTimeRangeVerifier.getMachedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb2, "3d2h");
  }

  @Test
  public void testEmptySpace() {
    String timeString = "Identity.* :   1d2h ; BizProfile.BizCompany : 3d";

    String lb1 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMachedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb2, "3d");
    String lb3 = CompactionTimeRangeVerifier.getMachedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb3, "2d");

    timeString = "2d;Identity.MemberAccount  :1d2h;   BizProfile.BizCompany:3d";
    String lb4 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb4, "1d2h");
    String lb5 = CompactionTimeRangeVerifier.getMachedLookbackTime("BizProfile/BizCompany", timeString, "2d");
    Assert.assertEquals(lb5, "3d");
    String lb6 = CompactionTimeRangeVerifier.getMachedLookbackTime("ABC/Unknown", timeString, "2d");
    Assert.assertEquals(lb6, "2d");
  }

  @Test
  public void testPartialMatchedNames() {
    String timeString = "Identity.Member$ :   1d2h";
    String lb1 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/Member", timeString, "2d");
    Assert.assertEquals(lb1, "1d2h");
    String lb2 = CompactionTimeRangeVerifier.getMachedLookbackTime("Identity/MemberAccount", timeString, "2d");
    Assert.assertEquals(lb2, "2d");
  }
}
