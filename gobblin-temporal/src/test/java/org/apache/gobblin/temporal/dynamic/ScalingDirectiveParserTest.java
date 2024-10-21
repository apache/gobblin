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

package org.apache.gobblin.temporal.dynamic;

import java.util.Optional;
import com.google.common.collect.Lists;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.Assert;


public class ScalingDirectiveParserTest {

  private final ScalingDirectiveParser parser = new ScalingDirectiveParser();

  @Test
  public void parseSimpleDirective() {
    ScalingDirective sd = parser.parse("1728435970.my_profile=24");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728435970L);
    Assert.assertEquals(sd.getProfileName(), "my_profile");
    Assert.assertEquals(sd.getSetPoint(), 24);
    Assert.assertFalse(sd.getOptDerivedFrom().isPresent());
  }

  @Test
  public void parseUnnamedBaselineProfile() {
    ScalingDirective sd = parser.parse("1728436821.=12");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728436821L);
    Assert.assertEquals(sd.getProfileName(), WorkforceProfiles.BASELINE_NAME);
    Assert.assertEquals(sd.getSetPoint(), 12);
    Assert.assertFalse(sd.getOptDerivedFrom().isPresent());
  }

  @Test
  public void parseBaselineProfile() {
    ScalingDirective sd = parser.parse("1728436828.baseline()=6");
    Assert.assertEquals(sd, new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 6, 1728436828L, Optional.empty()));
  }

  @Test
  public void parseAddingOverlayWithCommaSep() {
    ScalingDirective sd = parser.parse("1728439210.new_profile=16,bar+(a.b.c=7,l.m=sixteen)");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728439210L);
    Assert.assertEquals(sd.getProfileName(), "new_profile");
    Assert.assertEquals(sd.getSetPoint(), 16);
    Assert.assertTrue(sd.getOptDerivedFrom().isPresent());
    ProfileDerivation derivation = sd.getOptDerivedFrom().get();
    Assert.assertEquals(derivation.getBasisProfileName(), "bar");
    Assert.assertEquals(derivation.getOverlay(), new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("a.b.c", "7"), new ProfileOverlay.KVPair("l.m", "sixteen"))));
  }

  @Test
  public void parseAddingOverlayWithSemicolonSep() {
    ScalingDirective sd = parser.parse("1728439223.new_profile=32;baz+( a.b.c=7 ; l.m.n.o=sixteen )");
    Assert.assertEquals(sd, new ScalingDirective("new_profile", 32, 1728439223L, "baz", new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("a.b.c", "7"), new ProfileOverlay.KVPair("l.m.n.o", "sixteen")))));
  }

  @Test
  public void parseAddingOverlayWithCommaSepUrlEncoded() {
    ScalingDirective sd = parser.parse("1728460832.new_profile=16,baa+(a.b.c=7,l.m=sixteen%2C%20again)");
    Assert.assertEquals(sd, new ScalingDirective("new_profile", 16, 1728460832L, "baa", new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("a.b.c", "7"), new ProfileOverlay.KVPair("l.m", "sixteen, again")))));
  }

  @Test
  public void parseRemovingOverlayWithCommaSep() {
    ScalingDirective sd = parser.parse("1728436436.other_profile=9,my_profile-( x , y.z )");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728436436L);
    Assert.assertEquals(sd.getProfileName(), "other_profile");
    Assert.assertEquals(sd.getSetPoint(), 9);
    Assert.assertTrue(sd.getOptDerivedFrom().isPresent());
    ProfileDerivation derivation = sd.getOptDerivedFrom().get();
    Assert.assertEquals(derivation.getBasisProfileName(), "my_profile");
    Assert.assertEquals(derivation.getOverlay(), new ProfileOverlay.Removing(Lists.newArrayList("x", "y.z")));
  }

  @Test
  public void parseRemovingOverlayWithSemicolonSep() {
    ScalingDirective sd = parser.parse("1728436499.other_profile=9;my_profile-(x.y;z.z)");
    Assert.assertEquals(sd, new ScalingDirective("other_profile", 9, 1728436499L, "my_profile",
        new ProfileOverlay.Removing(Lists.newArrayList("x.y", "z.z"))));
  }

  @Test
  public void parseAddingOverlayWithWhitespace() {
    ScalingDirective sd = parser.parse("  1728998877 .  another  = 999 ; wow +  ( t.r  = jump%20  ; cb.az =  foo%20#%20111  ) ");
    Assert.assertEquals(sd, new ScalingDirective("another", 999, 1728998877L, "wow", new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("t.r", "jump "),
            new ProfileOverlay.KVPair("cb.az", "foo # 111")))));
  }

  @Test
  public void parseRemovingOverlayWithWhitespace() {
    ScalingDirective sd = parser.parse(" 1728334455  .  also =  77  ,  really  - (  t.r ,  cb.az )  ");
    Assert.assertEquals(sd, new ScalingDirective("also", 77, 1728334455L, "really",
        new ProfileOverlay.Removing(Lists.newArrayList("t.r", "cb.az"))));
  }

  @Test
  public void parseAddingOverlayWithUnnamedBaselineProfile() {
    ScalingDirective sd = parser.parse("1728441200.plus_profile=16,+(q.r.s=four,l.m=16)");
    Assert.assertEquals(sd, new ScalingDirective("plus_profile", 16, 1728441200L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Adding(
            Lists.newArrayList(new ProfileOverlay.KVPair("q.r.s", "four"), new ProfileOverlay.KVPair("l.m", "16")))));
  }

  @Test
  public void parseAddingOverlayWithBaselineProfile() {
    ScalingDirective sd = parser.parse("1728443640.plus_profile=16,baseline()+(q.r=five,l.m=12)");
    Assert.assertEquals(sd, new ScalingDirective("plus_profile", 16, 1728443640L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Adding(
            Lists.newArrayList(new ProfileOverlay.KVPair("q.r", "five"), new ProfileOverlay.KVPair("l.m", "12")))));
  }

  @Test
  public void parseRemovingOverlayWithUnnamedBaselineProfile() {
    ScalingDirective sd = parser.parse("1728448521.extra_profile=0,-(a.b, c.d)");
    Assert.assertEquals(sd, new ScalingDirective("extra_profile", 0, 1728448521L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Removing(Lists.newArrayList("a.b", "c.d"))));
  }

  @Test
  public void parseRemovingOverlayWithBaselineProfile() {
    ScalingDirective sd = parser.parse("4.extra_profile=9,baseline()-(a.b, c.d)");
    Assert.assertEquals(sd, new ScalingDirective("extra_profile", 9, 4L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Removing(Lists.newArrayList("a.b", "c.d"))));
  }


  @DataProvider(name = "funkyButValidDirectives")
  public String[][] validDirectives() {
    return new String[][]{
        // null overlay upon unnamed baseline profile (null overlay functions as the 'identity element'):
        {"1728435970.my_profile=24,+()"},
        {"1728435970.my_profile=24,-()"},
        {"1728435970.my_profile=24;+()"},
        {"1728435970.my_profile=24;-()"},

        // null overlay upon named profile:
        {"1728435970.my_profile=24,foo+()"},
        {"1728435970.my_profile=24,foo-()"},
        {"1728435970.my_profile=24;foo+()"},
        {"1728435970.my_profile=24;foo-()"},

        // seemingly separator mismatch, but in fact the NOT-separator is part of the value (e.g. a="7;m=sixteen"):
        { "1728439210.new_profile=16,bar+(a=7;m=sixteen)" },
        { "1728439210.new_profile=16;bar+(a=7,m=sixteen)" },
        { "1728439210.new_profile=16,bar+(a=7;)" },
        { "1728439210.new_profile=16;bar+(a=7,)" }

        // NOTE: unlike Adding, separator mismatch causes failure with the Removing overlay, because the NOT-separator is illegal in a key
    };
  }

  @Test(
      expectedExceptions = {},
      dataProvider = "funkyButValidDirectives"
  )
  public void parseValidDirectives(String directive) {
    Assert.assertNotNull(parser.parse(directive));
  }


  @DataProvider(name = "invalidDirectives")
  public String[][] invalidDirectives() {
    return new String[][] {
        // invalid values:
        { "invalid_timestamp.my_profile=24" },
        { "1728435970.my_profile=invalid_setpoint" },
        { "1728435970.my_profile=-15" },

        // incomplete/fragments:
        { "1728435970.my_profile=24," },
        { "1728435970.my_profile=24;" },
        { "1728435970.my_profile=24,+" },
        { "1728435970.my_profile=24,-" },
        { "1728435970.my_profile=24,foo+" },
        { "1728435970.my_profile=24,foo-" },
        { "1728435970.my_profile=24,foo+a=7" },
        { "1728435970.my_profile=24,foo-x" },

        // adding: invalid set-point + missing token examples:
        { "1728439210.new_profile=-6,bar+(a=7,m=sixteen)" },
        { "1728439210.new_profile=16,bar+(a=7,m=sixteen" },
        { "1728439210.new_profile=16,bar+a=7,m=sixteen)" },

        // adding: key, instead of key-value pair:
        { "1728439210.new_profile=16,bar+(a=7,m)" },
        { "1728439210.new_profile=16,bar+(a,m)" },

        // adding: superfluous separator or used incorrectly as a terminator:
        { "1728439210.new_profile=16,bar+(,)" },
        { "1728439210.new_profile=16;bar+(;)" },
        { "1728439210.new_profile=16,bar+(,,)" },
        { "1728439210.new_profile=16;bar+(;;)" },
        { "1728439210.new_profile=16,bar+(a=7,)" },
        { "1728439210.new_profile=16;bar+(a=7;)" },

        // removing: invalid set-point + missing token examples:
        { "1728436436.other_profile=-9,my_profile-(x)" },
        { "1728436436.other_profile=69,my_profile-(x" },
        { "1728436436.other_profile=69,my_profile-x)" },

        // removing: key-value pair instead of key:
        { "1728436436.other_profile=69,my_profile-(x=y,z)" },
        { "1728436436.other_profile=69,my_profile-(x=y,z=1)" },

        // removing: superfluous separator or used incorrectly as a terminator:
        { "1728436436.other_profile=69,my_profile-(,)" },
        { "1728436436.other_profile=69;my_profile-(;)" },
        { "1728436436.other_profile=69,my_profile-(,,)" },
        { "1728436436.other_profile=69;my_profile-(;;)" },
        { "1728436436.other_profile=69,my_profile-(x,)" },
        { "1728436436.other_profile=69;my_profile-(x;)" },

        // removing: seemingly separator mismatch, but in fact the NOT-separator is illegal in a key (e.g. "x;y"):
        { "1728436436.other_profile=69,my_profile-(x;y)" },
        { "1728436436.other_profile=69;my_profile-(x,y)" },
        { "1728436436.other_profile=69,my_profile-(x;)" },
        { "1728436436.other_profile=69;my_profile-(x,)" }
    };
  }

  @Test(
      expectedExceptions = ScalingDirectiveParser.MalformedDirectiveException.class,
      dataProvider = "invalidDirectives"
  )
  public void parseInvalidDirectives(String directive) {
    parser.parse(directive);
  }
}
