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

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.Assert;


/** Test {@link ScalingDirectiveParser} */
public class ScalingDirectiveParserTest {

  private final ScalingDirectiveParser parser = new ScalingDirectiveParser();

  @Test
  public void parseSimpleDirective() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728435970.my_profile=24");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728435970L);
    Assert.assertEquals(sd.getProfileName(), "my_profile");
    Assert.assertEquals(sd.getSetPoint(), 24);
    Assert.assertFalse(sd.getOptDerivedFrom().isPresent());
  }

  @Test
  public void parseUnnamedBaselineProfile() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728436821.=12");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728436821L);
    Assert.assertEquals(sd.getProfileName(), WorkforceProfiles.BASELINE_NAME);
    Assert.assertEquals(sd.getSetPoint(), 12);
    Assert.assertFalse(sd.getOptDerivedFrom().isPresent());
  }

  @Test
  public void parseBaselineProfilePseudoIdentifier() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728436828.baseline()=6");
    Assert.assertEquals(sd, new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 6, 1728436828L, Optional.empty()));
  }

  @Test
  public void parseAddingOverlayWithCommaSep() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728439210.new_profile=16,bar+(a.b.c=7,l.m=sixteen)");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728439210L);
    Assert.assertEquals(sd.getProfileName(), "new_profile");
    Assert.assertEquals(sd.getSetPoint(), 16);
    Assert.assertTrue(sd.getOptDerivedFrom().isPresent());
    ProfileDerivation derivation = sd.getOptDerivedFrom().get();
    Assert.assertEquals(derivation.getBasisProfileName(), "bar");
    Assert.assertEquals(derivation.getOverlay(),
        new ProfileOverlay.Adding(new ProfileOverlay.KVPair("a.b.c", "7"), new ProfileOverlay.KVPair("l.m", "sixteen")));
  }

  @Test
  public void parseAddingOverlayWithSemicolonSep() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728439223.new_profile=32;baz+( a.b.c=7 ; l.m.n.o=sixteen )");
    Assert.assertEquals(sd, new ScalingDirective("new_profile", 32, 1728439223L, "baz",
        new ProfileOverlay.Adding(new ProfileOverlay.KVPair("a.b.c", "7"), new ProfileOverlay.KVPair("l.m.n.o", "sixteen"))));
  }

  @Test
  public void parseAddingOverlayWithCommaSepUrlEncoded() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728460832.new_profile=16,baa+(a.b.c=7,l.m=sixteen%2C%20again)");
    Assert.assertEquals(sd, new ScalingDirective("new_profile", 16, 1728460832L, "baa",
        new ProfileOverlay.Adding(new ProfileOverlay.KVPair("a.b.c", "7"), new ProfileOverlay.KVPair("l.m", "sixteen, again"))));
  }

  @Test
  public void parseRemovingOverlayWithCommaSep() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728436436.other_profile=9,my_profile-( x , y.z )");
    Assert.assertEquals(sd.getTimestampEpochMillis(), 1728436436L);
    Assert.assertEquals(sd.getProfileName(), "other_profile");
    Assert.assertEquals(sd.getSetPoint(), 9);
    Assert.assertTrue(sd.getOptDerivedFrom().isPresent());
    ProfileDerivation derivation = sd.getOptDerivedFrom().get();
    Assert.assertEquals(derivation.getBasisProfileName(), "my_profile");
    Assert.assertEquals(derivation.getOverlay(), new ProfileOverlay.Removing("x", "y.z"));
  }

  @Test
  public void parseRemovingOverlayWithSemicolonSep() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728436499.other_profile=9;my_profile-(x.y;z.z)");
    Assert.assertEquals(sd, new ScalingDirective("other_profile", 9, 1728436499L, "my_profile",
        new ProfileOverlay.Removing("x.y", "z.z")));
  }

  @Test
  public void parseAddingOverlayWithWhitespace() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("  1728998877 .  another  = 999 ; wow +  ( t.r  = jump%20  ; cb.az =  foo%20#%20111  ) ");
    Assert.assertEquals(sd, new ScalingDirective("another", 999, 1728998877L, "wow",
        new ProfileOverlay.Adding(new ProfileOverlay.KVPair("t.r", "jump "), new ProfileOverlay.KVPair("cb.az", "foo # 111"))));
  }

  @Test
  public void parseRemovingOverlayWithWhitespace() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse(" 1728334455  .  also =  77  ,  really  - (  t.r ,  cb.az )  ");
    Assert.assertEquals(sd, new ScalingDirective("also", 77, 1728334455L, "really",
        new ProfileOverlay.Removing("t.r", "cb.az")));
  }

  @Test
  public void parseAddingOverlayUponUnnamedBaselineProfile() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728441200.plus_profile=16,+(q.r.s=four,l.m=16)");
    Assert.assertEquals(sd, new ScalingDirective("plus_profile", 16, 1728441200L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Adding(new ProfileOverlay.KVPair("q.r.s", "four"), new ProfileOverlay.KVPair("l.m", "16"))));
  }

  @Test
  public void parseAddingOverlayUponBaselineProfilePseudoIdentifier() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728443640.plus_profile=16,baseline()+(q.r=five,l.m=12)");
    Assert.assertEquals(sd, new ScalingDirective("plus_profile", 16, 1728443640L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Adding(new ProfileOverlay.KVPair("q.r", "five"), new ProfileOverlay.KVPair("l.m", "12"))));
  }

  @Test
  public void parseRemovingOverlayUponUnnamedBaselineProfile() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("1728448521.extra_profile=0,-(a.b, c.d)");
    Assert.assertEquals(sd, new ScalingDirective("extra_profile", 0, 1728448521L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Removing("a.b", "c.d")));
  }

  @Test
  public void parseRemovingOverlayUponBaselineProfilePseudoIdentifier() throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse("4.extra_profile=9,baseline()-(a.b, c.d)");
    Assert.assertEquals(sd, new ScalingDirective("extra_profile", 9, 4L, WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Removing("a.b", "c.d")));
  }

  @Test
  public void parseProfileIdTooLongThrows() throws ScalingDirectiveParser.InvalidSyntaxException {
    BiFunction<String, String, String> fmtRemovingOverlaySyntax = (profileId, basisProfileId) -> {
      return "1728449000." + profileId + "=99," + basisProfileId + "-(foo,bar,baz)";
    };
    String alphabet = IntStream.rangeClosed('a', 'z').collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    String tooLongId = alphabet + alphabet.toUpperCase() + alphabet + alphabet.toUpperCase();
    Assert.assertTrue(tooLongId.length() > ScalingDirectiveParser.MAX_PROFILE_IDENTIFIER_LENGTH);

    final int atMaxLen = ScalingDirectiveParser.MAX_PROFILE_IDENTIFIER_LENGTH;
    final int beyondMaxLen = ScalingDirectiveParser.MAX_PROFILE_IDENTIFIER_LENGTH + 1;
    String notTooLongDirective1 = fmtRemovingOverlaySyntax.apply(tooLongId.substring(0, atMaxLen), "some_profile");
    String notTooLongDirective2 = fmtRemovingOverlaySyntax.apply("new_profile", tooLongId.substring(0, atMaxLen));
    String notTooLongDirective3 = fmtRemovingOverlaySyntax.apply(tooLongId.substring(0, atMaxLen), tooLongId.substring(1, atMaxLen + 1));

    for (String directiveStr : new String[] { notTooLongDirective1, notTooLongDirective2, notTooLongDirective3 }) {
      Assert.assertNotNull(parser.parse(directiveStr));
    }

    String tooLongDirective1 = fmtRemovingOverlaySyntax.apply(tooLongId.substring(0, beyondMaxLen), "some_profile");
    String tooLongDirective2 = fmtRemovingOverlaySyntax.apply("new_profile", tooLongId.substring(0, beyondMaxLen));
    String tooLongDirective3 = fmtRemovingOverlaySyntax.apply(tooLongId.substring(0, beyondMaxLen), tooLongId.substring(1, beyondMaxLen + 1));

    Arrays.stream(new String[] { tooLongDirective1, tooLongDirective2, tooLongDirective3 }).forEach(directiveStr -> {
      Assert.assertThrows(ScalingDirectiveParser.InvalidSyntaxException.class, () -> parser.parse(directiveStr));
    });
  }

  @DataProvider(name = "funkyButValidDirectives")
  public String[][] validDirectives() {
    return new String[][]{
        // null overlay upon unnamed baseline profile (null overlay functions as the 'identity element'):
        { "1728435970.my_profile=24,+()" },
        { "1728435970.my_profile=24,-()" },
        { "1728435970.my_profile=24;+()" },
        { "1728435970.my_profile=24;-()" },

        // null overlay upon named profile:
        { "1728435970.my_profile=24,foo+()" },
        { "1728435970.my_profile=24,foo-()" },
        { "1728435970.my_profile=24;foo+()" },
        { "1728435970.my_profile=24;foo-()" },

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
  public void parseValidDirectives(String directive) throws ScalingDirectiveParser.InvalidSyntaxException {
    Assert.assertNotNull(parser.parse(directive));
  }

  @DataProvider(name = "validDirectivesToRoundTripWithAsString")
  public String[][] validDirectivesToRoundTripWithAsString() {
    return new String[][]{
        { "2.some_profile=15" },
        { "6.extra_profile=9,the_basis+(a.b=foo, c.d=bar)" },
        { "6.extra_profile=9,the_basis-(a.b, c.d)" },
        // funky ones:
        { "1728435970.my_profile=24,+()" },
        { "1728435970.my_profile=24,-()" },
        { "1728435970.my_profile=24,foo+()" },
        { "1728435970.my_profile=24,foo-()" }
    };
  }

  @Test(
      expectedExceptions = {},
      dataProvider = "validDirectivesToRoundTripWithAsString"
  )
  public void roundTripAsStringFollowingSuccessfulParse(String directive) throws ScalingDirectiveParser.InvalidSyntaxException {
    Assert.assertEquals(ScalingDirectiveParser.asString(parser.parse(directive)), directive);
  }

  @DataProvider(name = "validDirectivesWithOverlayPlaceholder")
  public String[][] validDirectivesWithOverlayPlaceholder() {
    return new String[][]{
        { "6.extra_profile=9,the_basis+(...)" },
        { "6.extra_profile=9;the_basis+(...)" },
        { "6.extra_profile=9,the_basis-(...)" },
        { "6.extra_profile=9;the_basis-(...)" }
    };
  }

  @Test(
      expectedExceptions = ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition.class,
      dataProvider = "validDirectivesWithOverlayPlaceholder"
  )
  public void parseDirectivesWithPlaceholderThrowsOverlayPlaceholderNeedsDefinition(String directive) throws ScalingDirectiveParser.InvalidSyntaxException {
    Assert.assertEquals(ScalingDirectiveParser.asString(parser.parse(directive)), directive);
  }

  @DataProvider(name = "stringifiedForAsStringWithPlaceholderPlusOverlay")
  public Object[][] directivesForAsStringWithPlaceholderPlusOverlay() {
    return new Object[][]{
        { "1728435970.my_profile=24", "1728435970.my_profile=24", "" },
        { "1728439210.new_profile=16,bar+(a.b.c=7, l.m=sixteen)", "1728439210.new_profile=16,bar+(...)", "a.b.c=7, l.m=sixteen" },
        { "1728436436.other_profile=9,my_profile-(x, y.z)", "1728436436.other_profile=9,my_profile-(...)", "x, y.z" }
    };
  }

  @Test(
      expectedExceptions = {},
      dataProvider = "stringifiedForAsStringWithPlaceholderPlusOverlay"
  )
  public void testAsStringWithPlaceholderPlusSeparateOverlay(String directive, String expectedString, String expectedOverlay)
      throws ScalingDirectiveParser.InvalidSyntaxException {
    ScalingDirective sd = parser.parse(directive);
    ScalingDirectiveParser.StringWithPlaceholderPlusOverlay result = ScalingDirectiveParser.asStringWithPlaceholderPlusOverlay(sd);
    Assert.assertEquals(result.getDirectiveStringWithPlaceholder(), expectedString);
    Assert.assertEquals(result.getOverlayDefinitionString(), expectedOverlay);

    // verify round-trip back to the original:
    try {
      parser.parse(result.getDirectiveStringWithPlaceholder());
    } catch (ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition needsDefinition) {
      Assert.assertEquals(ScalingDirectiveParser.asString(needsDefinition.retryParsingWithDefinition(expectedOverlay)), directive);
    }
  }

  @DataProvider(name = "overlayPlaceholderDirectivesWithCompletionDefAndEquivalent")
  public String[][] overlayPlaceholderDirectivesWithCompletionDefAndEquivalent() {
    return new String[][]{
        { "6.extra_profile=9,the_basis+(...)", "a=7,m=sixteen", "6.extra_profile=9,the_basis+(a=7,m=sixteen)" },
        { "6.extra_profile=9,the_basis+(...)", "a=7;m=sixteen", "6.extra_profile=9,the_basis+(a=7%3Bm%3Dsixteen)" }, // sep mismatch, so val == "7;m=sixteen"
        { "6.extra_profile=9,the_basis+(...)", "a.b.c=7,l.m=sixteen%2C%20again", "6.extra_profile=9,the_basis+(a.b.c=7,l.m=sixteen%2C%20again)" },
        { "6.extra_profile=9;the_basis+(...)", "a=7,m=sixteen", "6.extra_profile=9;the_basis+(a=7%2Cm%3Dsixteen)" }, // sep mismatch, so val == "7,m=sixteen"
        { "6.extra_profile=9;the_basis+(...)", "a=7;m=sixteen", "6.extra_profile=9;the_basis+(a=7;m=sixteen)" },
        { "6.extra_profile=9;the_basis+(...)", "a.b.c=7;l.m=sixteen%2C%20again", "6.extra_profile=9;the_basis+(a.b.c=7;l.m=sixteen%2C%20again)" },
        { "6.extra_profile=9,the_basis-(...)", "a.b,x.y.z", "6.extra_profile=9,the_basis-(a.b,x.y.z)" },
        { "6.extra_profile=9,the_basis-(...)", "x,y.z", "6.extra_profile=9,the_basis-(x,y.z)" },
        { "6.extra_profile=9;the_basis-(...)", "x;y.z", "6.extra_profile=9;the_basis-(x;y.z)" },
        { "6.extra_profile=9;the_basis-(...)", "a.b;x.y.z", "6.extra_profile=9;the_basis-(a.b;x.y.z)" }
    };
  }

  @Test(
      expectedExceptions = {},
      dataProvider = "overlayPlaceholderDirectivesWithCompletionDefAndEquivalent"
  )
  public void verifyOverlayPlaceholderEquivalence(String directiveWithPlaceholder, String overlayDefinition, String equivDirective)
      throws ScalingDirectiveParser.InvalidSyntaxException {
    try {
      parser.parse(directiveWithPlaceholder);
      Assert.fail("Expected `ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition` due to the placeholder in the directive");
    } catch (ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition needsDefinition) {
      ScalingDirective directive = needsDefinition.retryParsingWithDefinition(overlayDefinition);
      Assert.assertEquals(directive, parser.parse(equivDirective));
    }
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

        // adding: overlay placeholder may not be used with key-value pairs:
        { "1728439210.new_profile=16,bar+(a=7,...)" },
        { "1728439210.new_profile=16,bar+(...,b=4)" },
        { "1728439210.new_profile=16,bar+(a=7,...,b=4)" },
        { "1728439210.new_profile=16;bar+(a=7;...)" },
        { "1728439210.new_profile=16;bar+(...;b=4)" },
        { "1728439210.new_profile=16;bar+(a=7;...;b=4)" },

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

        // removing: overlay placeholder may not be used with keys:
        { "1728436436.other_profile=69,my_profile-(x,...)" },
        { "1728436436.other_profile=69,my_profile-(...,z)" },
        { "1728436436.other_profile=69,my_profile-(x,...,z)" },
        { "1728436436.other_profile=69;my_profile-(x;...)" },
        { "1728436436.other_profile=69;my_profile-(...;z)" },
        { "1728436436.other_profile=69;my_profile-(x;...;z)" },

        // removing: seemingly separator mismatch, but in fact the NOT-separator is illegal in a key (e.g. "x;y"):
        { "1728436436.other_profile=69,my_profile-(x;y)" },
        { "1728436436.other_profile=69;my_profile-(x,y)" },
        { "1728436436.other_profile=69,my_profile-(x;)" },
        { "1728436436.other_profile=69;my_profile-(x,)" }
    };
  }

  @Test(
      expectedExceptions = ScalingDirectiveParser.InvalidSyntaxException.class,
      dataProvider = "invalidDirectives"
  )
  public void parseInvalidDirectivesThrows(String directive) throws ScalingDirectiveParser.InvalidSyntaxException {
    parser.parse(directive);
  }

  @DataProvider(name = "overlayPlaceholderDirectivesWithInvalidCompletionDef")
  public String[][] overlayPlaceholderDirectivesWithInvalidCompletionDef() {
    return new String[][]{
        { "6.extra_profile=9,the_basis+(...)", "..." },
        { "6.extra_profile=9;the_basis+(...)", "..." },
        { "6.extra_profile=9,the_basis+(...)", "a=7," },
        { "6.extra_profile=9;the_basis+(...)", "a=7;" },
        { "6.extra_profile=9,the_basis+(...)", "a.b.c,l.m.n" },
        { "6.extra_profile=9;the_basis+(...)", "a.b.c;l.m.n" },
        { "6.extra_profile=9,the_basis-(...)", "..." },
        { "6.extra_profile=9;the_basis-(...)", "..." },
        { "6.extra_profile=9,the_basis-(...)", "a.b," },
        { "6.extra_profile=9;the_basis-(...)", "a.b;" },
        { "6.extra_profile=9,the_basis-(...)", "x=foo,y.z=bar" },
        { "6.extra_profile=9;the_basis-(...)", "x=foo;y.z=bar" }
    };
  }

  @Test(
      expectedExceptions = ScalingDirectiveParser.InvalidSyntaxException.class,
      dataProvider = "overlayPlaceholderDirectivesWithInvalidCompletionDef"
  )
  public void parsePlaceholderDefWithInvalidPlaceholderThrows(String directiveWithPlaceholder, String invalidOverlayDefinition)
      throws ScalingDirectiveParser.InvalidSyntaxException {
    try {
      parser.parse(directiveWithPlaceholder);
      Assert.fail("Expected `ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition` due to the placeholder in the directive");
    } catch (ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition needsDefinition) {
      Assert.assertNotNull(needsDefinition.retryParsingWithDefinition(invalidOverlayDefinition));
    }
  }
}
