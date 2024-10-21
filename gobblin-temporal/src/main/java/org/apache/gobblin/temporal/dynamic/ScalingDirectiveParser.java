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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  public static class MalformedDirectiveException extends IllegalArgumentException {
    private final String directive;
    public MalformedDirectiveException(String directive, String desc) {
      super("error: " + desc + ", in ==>" + directive + "<==");
      this.directive = directive;
    }
  }

  private static final String DIRECTIVE_REGEX = "(?x) \\s* (\\d+) \\s* \\. \\s* (\\w* | baseline\\(\\)) \\s* = \\s* (\\d+) "
      + "(?: \\s* ([;,]) \\s* (\\w* | baseline\\(\\)) \\s* (?: (\\+ \\s* \\( \\s* ([^)]*?) \\s* \\) ) | (- \\s* \\( \\s* ([^)]*?) \\s* \\) ) ) )? \\s*";

  private static final String KEY_REGEX = "(\\w+(?:\\.\\w+)*)";
  private static final String KEY_VALUE_REGEX = KEY_REGEX + "\\s*=\\s*(.*)";
  private static final Pattern directivePattern = Pattern.compile(DIRECTIVE_REGEX);
  private static final Pattern keyPattern = Pattern.compile(KEY_REGEX);
  private static final Pattern keyValuePattern = Pattern.compile(KEY_VALUE_REGEX);

  private static final String BASELINE_ID = "baseline()";

  public ScalingDirective parse(String directive) {
    Matcher parsed = directivePattern.matcher(directive);
    if (parsed.matches()) {
      long timestamp = Long.parseLong(parsed.group(1));
      String profileId = parsed.group(2);
      String profileName = identifyProfileName(profileId);
      int setpoint = Integer.parseInt(parsed.group(3));
      Optional<ProfileDerivation> optDerivedFrom = Optional.empty();
      String overlayIntroSep = parsed.group(4);
      if (overlayIntroSep != null) {
        String basisProfileName = identifyProfileName(parsed.group(5));
        if (parsed.group(6) != null) {  // '+' == adding
          List<ProfileOverlay.KVPair> additions = new ArrayList<>();
          String additionsStr = parsed.group(7);
          if (!additionsStr.equals("")) {
            for (String addStr : additionsStr.split("\\s*" + overlayIntroSep + "\\s*", -1)) {  // (negative limit to disallow trailing empty strings)
              Matcher keyValueParsed = keyValuePattern.matcher(addStr);
              if (keyValueParsed.matches()) {
                additions.add(new ProfileOverlay.KVPair(keyValueParsed.group(1), urlDecode(directive, keyValueParsed.group(2))));
              } else {
                throw new MalformedDirectiveException(directive, "unable to parse key-value pair - {{" + addStr + "}}");
              }
            }
          }
          optDerivedFrom = Optional.of(new ProfileDerivation(basisProfileName, new ProfileOverlay.Adding(additions)));
        } else {  // '-' == removing
          List<String> removalKeys = new ArrayList<>();
          String removalsStr = parsed.group(9);
          if (!removalsStr.equals("")) {
            for (String removeStr : removalsStr.split("\\s*" + overlayIntroSep + "\\s*", -1)) {  // (negative limit to disallow trailing empty strings)
              Matcher keyParsed = keyPattern.matcher(removeStr);
              if (keyParsed.matches()) {
                removalKeys.add(keyParsed.group(1));
              } else {
                throw new MalformedDirectiveException(directive, "unable to parse key - {{" + removeStr + "}}");
              }
            }
          }
          optDerivedFrom = Optional.of(new ProfileDerivation(basisProfileName, new ProfileOverlay.Removing(removalKeys)));
        }
      }
      return new ScalingDirective(profileName, setpoint, timestamp, optDerivedFrom);
    } else {
      throw new MalformedDirectiveException(directive, "invalid syntax");
    }
  }

  private static String identifyProfileName(String profileId) {
    return profileId.equals(BASELINE_ID) ? WorkforceProfiles.BASELINE_NAME : profileId;
  }

  private static String urlDecode(String directive, String s) {
    try {
      return java.net.URLDecoder.decode(s, "UTF-8");
    } catch (java.io.UnsupportedEncodingException e) {
      throw new MalformedDirectiveException(directive, "unable to URL-decode - {{" + s + "}}");
    }
  }
}
