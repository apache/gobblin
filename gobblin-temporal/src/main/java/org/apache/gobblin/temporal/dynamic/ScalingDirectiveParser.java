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

import lombok.extern.slf4j.Slf4j;


/**
 * parse {@link ScalingDirective}s with syntax of the form:
 * TIMESTAMP '.' WORKER_NAME '=' SETPOINT [ ( ',' | ';' ) WORKER_NAME ( '+(' KV_PAIR (*SEP* KV_PAIR)* ')' | '-( KEY (*SEP* KEY* ')' ) ]
 * where *SEP* is either ',' or ';' (whichever did follow SETPOINT)
 * the first form with '+' is an "adding" (upsert) overlay, the second form with '-' is a removing overlay
 * allows for URL-encoded values in the KV_PAIRs and whitespace around any token
 *
    1728435970.my_profile=24
    1728436821.=24
    1728436828.baseline()=24

    1728439210.new_profile=16,bar+(a.b.c=7,l.m=sixteen)
    1728439223.new_profile=16;bar+(a.b.c=7;l.m=sixteen)
    1728460832.new_profile=16,bar+(a.b.c=7,l.m=sixteen%2C%20again)

    1728436436.other_profile=9,my_profile-(x,y.z)
    1728436499.other_profile=9;my_profile-(x;y.z)

    1728441200.plus_profile=16,+(a.b.c=7,l.m=sixteen)
    1728443640.plus_profile=16,baseline()+(a.b.c=7,l.m=sixteen)

    1728448521.extra_profile=9,-(a.b, c.d)
    1728449978.extra_profile=9,baseline()-(a.b, c.d)
*/
@Slf4j
public class ScalingDirectiveParser {
  public static class InvalidSyntaxException extends Exception {
    private final String directive;
    public InvalidSyntaxException(String directive, String desc) {
      super("error: " + desc + ", in ==>" + directive + "<==");
      this.directive = directive;
    }
  }

  public static class OverlayPlaceholderNeedsDefinition extends RuntimeException {
    private final String directive;
    private final String overlaySep;
    private final boolean isAdding;
    // ATTENTION: explicitly managed, rather than making this a non-static inner class so `definePlaceholder` may be `static` for testing, while avoiding:
    //   Static declarations in inner classes are not supported at language level '8'
    private final ScalingDirectiveParser parser;

    private OverlayPlaceholderNeedsDefinition(String directive, String overlaySep, boolean isAdding, ScalingDirectiveParser enclosing) {
      super("overlay placeholder, in ==>" + directive + "<==");
      this.directive = directive;
      this.overlaySep = overlaySep;
      this.isAdding = isAdding;
      this.parser = enclosing;
    }

    // doesn't allow recursive placeholding...
    public ScalingDirective retryParsingWithDefinition(String overlayDefinition) throws InvalidSyntaxException {
      try {
        return this.parser.parse(definePlaceholder(this.directive, this.overlaySep, this.isAdding, overlayDefinition));
      } catch (OverlayPlaceholderNeedsDefinition e) {
        throw new InvalidSyntaxException(this.directive, "overlay placeholder definition must not be itself another placeholder");
      }
    }

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

      // correct any double-encoding of '%', in case it arrived url-encoded
      return directiveWithPlaceholder.replace(OVERLAY_DEFINITION_PLACEHOLDER, urlEncodedOverlayDef.replace("%25", "%"));
    }
  }


  // TODO: also support non-inline overlay definitions - "(...)"
  //   consider an additional trailing "|" (or "," / ";") syntax when the additional props are only needed post-launch
  // since we're primarily designed for HDFS file names, in addition, a max identifier length (to fit within HDFS path segment limit == 255)
  // org.apache.hadoop.hdfs.protocol.FSLimitException$PathComponentTooLongException: \
  //   The maximum path component name limit of ... in directory ... is exceeded: limit=255 length=256
  // 200 = 255 [limit] - 16 [digit timestamp] - 1 ['.'] - 1 ['='] - 1 [',' / ';'] - 6 ['+(...)' / '-(...)'] - 30 [reserved... for future]
  // current millis-precision epoch timestamp requires 10 chars, but we reserve 16 for future-proofing to nanos-precision
  // hence, neither (of the max two) profile identifiers may exceed 100 chars.
  // TODO: syntax to indicate removing an attr during an addition
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
            for (String addStr : additionsStr.split("\\s*" + overlayIntroSep + "\\s*", -1)) {  // (negative limit to disallow trailing empty strings)
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
            for (String removeStr : removalsStr.split("\\s*" + overlayIntroSep + "\\s*", -1)) {  // (negative limit to disallow trailing empty strings)
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

  private static String identifyProfileName(String profileId, String directive) throws InvalidSyntaxException {
    if (profileId.length() > MAX_PROFILE_IDENTIFIER_LENGTH) {
      throw new InvalidSyntaxException(directive, "profile ID exceeds length limit (of " + MAX_PROFILE_IDENTIFIER_LENGTH + "): '" + profileId + "'");
    }
    return profileId.equals(BASELINE_ID) ? WorkforceProfiles.BASELINE_NAME : profileId;
  }

  private static String urlDecode(String directive, String s) throws InvalidSyntaxException {
    try {
      return java.net.URLDecoder.decode(s, URL_ENCODING_CHARSET);
    } catch (java.io.UnsupportedEncodingException e) {
      throw new InvalidSyntaxException(directive, "unable to URL-decode - {{" + s + "}}");
    }
  }

  private static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, URL_ENCODING_CHARSET);
    } catch (java.io.UnsupportedEncodingException e) {
      throw new RuntimeException("THIS SHOULD BE IMPOSSIBLE, given we used '" + URL_ENCODING_CHARSET + "' with {{" + s + "}}", e);
    }
  }
}
