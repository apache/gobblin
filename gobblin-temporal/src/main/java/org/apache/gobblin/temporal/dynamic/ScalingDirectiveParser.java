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

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Parser for {@link ScalingDirective} syntax of the form:
 *   TIMESTAMP '.' PROFILE_NAME '=' SET_POINT [ ( ',' | ';' ) PROFILE_NAME ( '+(' KV_PAIR (<SEP> KV_PAIR)* ')' | '-( KEY (<SEP> KEY)* ')' ) ]
 * where:
 *   only ( TIMESTAMP '.' PROFILE_NAME '=' SET_POINT ) are non-optional
 *
 *   <SEP> is either ',' or ';' (whichever did follow SET_POINT)
 *
 *   TIMESTAMP is millis-since-epoch
 *
 *   PROFILE_NAME is a simple [a-zA-Z0-9_]+ identifier familiar from many programming languages.  The special name "baseline()" is reserved
 *   for the baseline profile, which may alternatively be spelled as the empty identifier ("").
 *
 *   SET_POINT must be a non-negative integer ('0' indicates no instances desired).
 *
 *   The first form introduced by '+' is an "adding" (upsert) overlay; the second form with '-' is a "removing" overlay.
 *
 *   KV_PAIR (for "adding") is an '='-delimited (KEY '=' VALUE), where VALUE may use URL-encoding to escape characters.
 *
 *   KEY (for "removing" and in the "adding" KV_PAIR) is a '.'-separated sequence of alphanumeric identifier segments, as in a {@link java.util.Properties}
 *   or {@link com.typesafe.config.Config} name.
 *
 *   Whitespace may appear around any tokens, though not within a KEY or a VALUE.  Comments are unsupported.
 *
 * As an alternative to inlining a lengthy adding or removing overlay definition, {@link #OVERLAY_DEFINITION_PLACEHOLDER} may stand in to indicate that
 * the definition itself will be supplied separately.  Supply it and {@link OverlayPlaceholderNeedsDefinition#retryParsingWithDefinition(String)}, upon
 * the same UNCHECKED exception (originally thrown by {@link #parse(String)}).
 *
 * Given this syntax is specifically designed for directives to appear as an HDFS file name, we enforce a {@link #MAX_PROFILE_IDENTIFIER_LENGTH} (== 100),
 * to fit within the HDFS path segment limit (== 255), and therein avoid:
 *   org.apache.hadoop.hdfs.protocol.FSLimitException$PathComponentTooLongException: \
 *       The maximum path component name limit of ... in directory ... is exceeded: limit=255 length=256
 * the max identifier length of 100 is chosen as follows:
 *   - limit == 255
 *   - current millis-precision epoch timestamp requires 10 chars, yet reserve 16 for future-proofing to nanos-precision
 *   - reserve 30 chars for future use in syntax evolution
 *   - 200 = 255 [limit] - 16 [digit timestamp] - 1 ['.'] - 1 ['='] - 1 [',' / ';'] - 6 ['+(...)' / '-(...)'] - 30 [reserved... for future]
 *   - since a max of two profile identifiers, neither may exceed (200 / 2 == 100) chars
 *
 * Examples:
 *  - simply update the set point for the profile, `my_profile` (already existing/defined):
 *      1728435970.my_profile=24
 *
 *  - update the set point of the baseline profile (equiv. forms):
 *      1728436821.=24
 *      1728436828.baseline()=24
 *
 *  - define a new profile, `new_profile`, with a set point of 16 by deriving via "adding" overlay from the existing profile, `bar` (equiv. forms):
 *      1728439210.new_profile=16,bar+(a.b.c=7,l.m=sixteen)
 *      1728439223.new_profile=16;bar+(a.b.c=7;l.m=sixteen)
 *
 *  - similar derivation, but demonstrating URL-encoding (to include ',' and literal space in the value):
 *      1728460832.new_profile=16,bar+(a.b.c=7,l.m=sixteen%2C%20again)
 *
 *  - define a new profile, `other_profile`, with a set point of 9 by deriving via "removing" overlay from the existing profile, `my_profile` (equiv. forms):
 *      1728436436.other_profile=9,my_profile-(x,y.z)
 *      1728436499.other_profile=9;my_profile-(x;y.z)
 *
 *  - define a new profile, `plus_profile`, with an initial set point, via "adding" overlay upon the baseline profile (equiv. forms):
 *      1728441200.plus_profile=5,+(a.b.c=7,l.m=sixteen)
 *      1728443640.plus_profile=5,baseline()+(a.b.c=7,l.m=sixteen)
 *
 *  - define a new profile, `extra_profile`, with an initial set point, via "removing" overlay upon the baseline profile (equiv. forms):
 *      1728448521.extra_profile=14,-(a.b, c.d)
 *      1728449978.extra_profile=14,baseline()-(a.b, c.d)
 *
 *  - define a new profile with an initial set point, using {@link #OVERLAY_DEFINITION_PLACEHOLDER} syntax instead of inlining the overlay definition:
 *      1728539479.and_also=21,baaz-(...)
 *      1728547230.this_too=19,quux+(...)
 */
@Slf4j
public class ScalingDirectiveParser {

  /** Announce a syntax error within {@link #getDirective()} */
  public static class InvalidSyntaxException extends Exception {
    @Getter
    private final String directive;

    public InvalidSyntaxException(String directive, String desc) {
      super("error: " + desc + ", in ==>" + directive + "<==");
      this.directive = directive;
    }
  }

  /**
   * Report that {@link #getDirective()} used {@link #OVERLAY_DEFINITION_PLACEHOLDER} in lieu of inlining an "adding" or "removing" overlay definition.
   *
   * When the overlay definition is later recovered, pass it to {@link #retryParsingWithDefinition(String)} to re-attempt the parse.
   */
  public static class OverlayPlaceholderNeedsDefinition extends RuntimeException {
    @Getter
    private final String directive;
    private final String overlaySep;
    private final boolean isAdding;
    // ATTENTION: explicitly manage a reference to `parser`, despite it being the enclosing class instance, instead of making this a non-static inner class.
    // That allows `definePlaceholder` to be `static`, for simpler testability, while dodging:
    //   Static declarations in inner classes are not supported at language level '8'
    private final ScalingDirectiveParser parser;

    private OverlayPlaceholderNeedsDefinition(String directive, String overlaySep, boolean isAdding, ScalingDirectiveParser enclosing) {
      super("overlay placeholder, in ==>" + directive + "<==");
      this.directive = directive;
      this.overlaySep = overlaySep;
      this.isAdding = isAdding;
      this.parser = enclosing;
    }

    /**
     * Pass the missing `overlayDefinition` and re-attempt parsing.  This DOES NOT allow nested placeholding, so `overlayDefinition` may not
     * itself be/contain {@link #OVERLAY_DEFINITION_PLACEHOLDER}.
     *
     * @return the parsed {@link ScalingDirective} or throw {@link InvalidSyntaxException}
     */
    public ScalingDirective retryParsingWithDefinition(String overlayDefinition) throws InvalidSyntaxException {
      try {
        return this.parser.parse(definePlaceholder(this.directive, this.overlaySep, this.isAdding, overlayDefinition));
      } catch (OverlayPlaceholderNeedsDefinition e) {
        throw new InvalidSyntaxException(this.directive, "overlay placeholder definition must not be itself another placeholder");
      }
    }

    /** encapsulate the intricacies of splicing `overlayDefinition` into `directiveWithPlaceholder`, after attending to the necessary URL-encoding */
    @VisibleForTesting
    protected static String definePlaceholder(String directiveWithPlaceholder, String overlaySep, boolean isAdding, String overlayDefinition) {
      // use care to selectively `urlEncode` parts (but NOT the entire string), to avoid disrupting syntactic chars, like [,;=]
      String urlEncodedOverlayDef = Arrays.stream(overlayDefinition.split("\\s*" + overlaySep + "\\s*", -1)) // (neg. limit to disallow trailing empty strings)
          .map(kvPair -> {
            String[] kv = kvPair.split("\\s*=\\s*", 2);
            if (isAdding && kv.length > 1) {
              return kv[0] + '=' + urlEncode(kv[1]);
            } else {
              return kvPair;
            }
          }).collect(Collectors.joining(overlaySep));

      // undo any double-encoding of '%', in case `overlayDefinition` arrived URL-encoded
      return directiveWithPlaceholder.replace(OVERLAY_DEFINITION_PLACEHOLDER, urlEncodedOverlayDef.replace("%25", "%"));
    }
  }

  // TODO: syntax to remove an attr while ALSO "adding" (so not simply setting to the empty string) - [proposal: alt. form for KV_PAIR ::= ( KEY '|=|' )]

  // syntax as described in class-level javadoc:
  private static final String DIRECTIVE_REGEX = "(?x) (?s) ^ \\s* (\\d+) \\s* \\. \\s* (\\w* | baseline\\(\\)) \\s* = \\s* (\\d+) "
      + "(?: \\s* ([;,]) \\s* (\\w* | baseline\\(\\)) \\s* (?: (\\+ \\s* \\( \\s* ([^)]*?) \\s* \\) ) | (- \\s* \\( \\s* ([^)]*?) \\s* \\) ) ) )? \\s* $";

  public static final int MAX_PROFILE_IDENTIFIER_LENGTH = 100;
  public static final String URL_ENCODING_CHARSET = "UTF-8";
  public static final String OVERLAY_DEFINITION_PLACEHOLDER = "...";

  private static final String KEY_REGEX = "(\\w+(?:\\.\\w+)*)";
  private static final String KEY_VALUE_REGEX = KEY_REGEX + "\\s*=\\s*(.*)";
  private static final Pattern directivePattern = Pattern.compile(DIRECTIVE_REGEX);
  private static final Pattern keyPattern = Pattern.compile(KEY_REGEX);
  private static final Pattern keyValuePattern = Pattern.compile(KEY_VALUE_REGEX);

  private static final String BASELINE_ID = "baseline()";

  /**
   * Parse `directive` into a {@link ScalingDirective} or throw {@link InvalidSyntaxException}
   *
   * When an overlay definition was not inlined because {@link #OVERLAY_DEFINITION_PLACEHOLDER} was used instead, throw the UNCHECKED exception
   * {@link OverlayPlaceholderNeedsDefinition}, which facilitates a subsequent attempt to supply the overlay definition and
   * {@link OverlayPlaceholderNeedsDefinition#retryParsingWithDefinition(String)} (a form of the Proxy pattern).
   */
  public ScalingDirective parse(String directive) throws InvalidSyntaxException {
    Matcher parsed = directivePattern.matcher(directive);
    if (parsed.matches()) {
      long timestamp = Long.parseLong(parsed.group(1));
      String profileId = parsed.group(2);
      String profileName = identifyProfileName(profileId, directive);
      int setpoint = Integer.parseInt(parsed.group(3));
      Optional<ProfileDerivation> optDerivedFrom = Optional.empty();
      String overlayIntroSep = parsed.group(4);
      if (overlayIntroSep != null) {
        String basisProfileName = identifyProfileName(parsed.group(5), directive);
        if (parsed.group(6) != null) {  // '+' == adding
          List<ProfileOverlay.KVPair> additions = new ArrayList<>();
          String additionsStr = parsed.group(7);
          if (additionsStr.equals(OVERLAY_DEFINITION_PLACEHOLDER)) {
            throw new OverlayPlaceholderNeedsDefinition(directive, overlayIntroSep, true, this);
          } else if (!additionsStr.equals("")) {
            for (String addStr : additionsStr.split("\\s*" + overlayIntroSep + "\\s*", -1)) {  // (`-1` limit to disallow trailing empty strings)
              Matcher keyValueParsed = keyValuePattern.matcher(addStr);
              if (keyValueParsed.matches()) {
                additions.add(new ProfileOverlay.KVPair(keyValueParsed.group(1), urlDecode(directive, keyValueParsed.group(2))));
              } else {
                throw new InvalidSyntaxException(directive, "unable to parse key-value pair - {{" + addStr + "}}");
              }
            }
          }
          optDerivedFrom = Optional.of(new ProfileDerivation(basisProfileName, new ProfileOverlay.Adding(additions)));
        } else {  // '-' == removing
          List<String> removalKeys = new ArrayList<>();
          String removalsStr = parsed.group(9);
          if (removalsStr.equals(OVERLAY_DEFINITION_PLACEHOLDER)) {
            throw new OverlayPlaceholderNeedsDefinition(directive, overlayIntroSep, false, this);
          } else if (!removalsStr.equals("")) {
            for (String removeStr : removalsStr.split("\\s*" + overlayIntroSep + "\\s*", -1)) {  // (`-1` limit to disallow trailing empty strings)
              Matcher keyParsed = keyPattern.matcher(removeStr);
              if (keyParsed.matches()) {
                removalKeys.add(keyParsed.group(1));
              } else {
                throw new InvalidSyntaxException(directive, "unable to parse key - {{" + removeStr + "}}");
              }
            }
          }
          optDerivedFrom = Optional.of(new ProfileDerivation(basisProfileName, new ProfileOverlay.Removing(removalKeys)));
        }
      }
      return new ScalingDirective(profileName, setpoint, timestamp, optDerivedFrom);
    } else {
      throw new InvalidSyntaxException(directive, "invalid syntax");
    }
  }

  /**
   * @return `directive` as a pretty-printed string
   *
   * NOTE: regardless of its length or content, the result inlines the entire overlay def, with {@link #OVERLAY_DEFINITION_PLACEHOLDER} NEVER used
   *
   * @see #parse(String), the inverse operation (approximately - modulo {@link #OVERLAY_DEFINITION_PLACEHOLDER}, noted above)
   */
  public static String asString(ScalingDirective directive) {
    StringBuilder sb = new StringBuilder();
    sb.append(directive.getTimestampEpochMillis()).append('.').append(directive.getProfileName()).append('=').append(directive.getSetPoint());
    directive.getOptDerivedFrom().ifPresent(derivedFrom -> {
      sb.append(',').append(derivedFrom.getBasisProfileName());
      sb.append(derivedFrom.getOverlay() instanceof ProfileOverlay.Adding ? "+(" : "-(");
      ProfileOverlay overlay = derivedFrom.getOverlay();
      if (overlay instanceof ProfileOverlay.Adding) {
        ProfileOverlay.Adding adding = (ProfileOverlay.Adding) overlay;
        for (ProfileOverlay.KVPair kv : adding.getAdditionPairs()) {
          sb.append(kv.getKey()).append('=').append(urlEncode(kv.getValue())).append(", ");
        }
        if (adding.getAdditionPairs().size() > 0) {
          sb.setLength(sb.length() - 2);  // remove trailing ", "
        }
      } else {
        ProfileOverlay.Removing removing = (ProfileOverlay.Removing) overlay;
        for (String key : removing.getRemovalKeys()) {
          sb.append(key).append(", ");
        }
        if (removing.getRemovalKeys().size() > 0) {
          sb.setLength(sb.length() - 2);  // remove trailing ", "
        }
      }
      sb.append(')');
    });
    return sb.toString();
  }

  /** handle special naming of {@link #BASELINE_ID} and enforce {@link #MAX_PROFILE_IDENTIFIER_LENGTH} */
  private static String identifyProfileName(String profileId, String directive) throws InvalidSyntaxException {
    if (profileId.length() > MAX_PROFILE_IDENTIFIER_LENGTH) {
      throw new InvalidSyntaxException(directive, "profile ID exceeds length limit (of " + MAX_PROFILE_IDENTIFIER_LENGTH + "): '" + profileId + "'");
    }
    return profileId.equals(BASELINE_ID) ? WorkforceProfiles.BASELINE_NAME : profileId;
  }

  /** @return `s`, URL-decoded as UTF-8 or throw {@link InvalidSyntaxException} */
  private static String urlDecode(String directive, String s) throws InvalidSyntaxException {
    try {
      return java.net.URLDecoder.decode(s, URL_ENCODING_CHARSET);
    } catch (java.io.UnsupportedEncodingException e) {
      throw new InvalidSyntaxException(directive, "unable to URL-decode - {{" + s + "}}");
    }
  }

  /** @return `s`, URL-encoded as UTF-8 and wrap any {@link java.io.UnsupportedEncodingException}--which SHOULD NEVER HAPPEN!--as an unchecked exception */
  private static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, URL_ENCODING_CHARSET);
    } catch (java.io.UnsupportedEncodingException e) {
      throw new RuntimeException("THIS SHOULD BE IMPOSSIBLE, given we used '" + URL_ENCODING_CHARSET + "' with {{" + s + "}}", e);
    }
  }
}
