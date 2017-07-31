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

package org.apache.gobblin.data.management.copy;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


/**
* Configuration for preserving attributes in Gobblin distcp jobs.
*/
@AllArgsConstructor
@EqualsAndHashCode
public class PreserveAttributes {

  public static final Pattern ATTRIBUTES_REGEXP = getAllowedRegexp();

  /**
   * Attributes that can be preserved.
   */
  public static enum Option {
    REPLICATION('r'),
    BLOCK_SIZE('b'),
    OWNER('u'),
    GROUP('g'),
    PERMISSION('p');

    private final char token;

    Option(char token) {
      this.token = token;
    }
  }

  private int options;

  /**
   * @return true if attribute should be preserved.
   */
  public boolean preserve(Option option) {
    return 0 < (this.options & (1 << option.ordinal()));
  }

  /**
   * Converts this instance of {@link PreserveAttributes} into a String that can be converted to an equivalent
   * {@link PreserveAttributes} using {@link PreserveAttributes#fromMnemonicString}. See the latter for more
   * information.
   * @return a String that can be converted to an equivalent {@link PreserveAttributes} using
   *         {@link PreserveAttributes#fromMnemonicString}
   */
  public String toMnemonicString() {
    int value = this.options;
    StringBuilder mnemonicString = new StringBuilder();
    for (Option option : Option.values()) {
      if (value % 2 != 0) {
        mnemonicString.append(option.token);
      }
      value >>= 1;
    }
    return mnemonicString.toString();
  }

  /**
   * Parse {@link PreserveAttributes} from a string of the form \[rbugp]*\:
   * * r -> preserve replication
   * * b -> preserve block size
   * * u -> preserve owner
   * * g -> preserve group
   * * p -> preserve permissions
   * Characters not in this character set will be ignored.
   *
   * @param s String of the form \[rbugp]*\
   * @return Parsed {@link PreserveAttributes}
   */
  public static PreserveAttributes fromMnemonicString(String s) {

    if (Strings.isNullOrEmpty(s)) {
      return new PreserveAttributes(0);
    }

    s = s.toLowerCase();

    Preconditions.checkArgument(ATTRIBUTES_REGEXP.matcher(s).matches(), "Invalid %s string %s, must be of the form %s.",
        PreserveAttributes.class.getSimpleName(), s, ATTRIBUTES_REGEXP.pattern());

    int value = 0;
    for (Option option : Option.values()) {
      if (s.indexOf(option.token) >= 0) {
        value |= 1 << option.ordinal();
      }
    }
    return new PreserveAttributes(value);
  }

  private static Pattern getAllowedRegexp() {
    StringBuilder builder = new StringBuilder("[");
    for (Option option : Option.values()) {
      builder.append(option.token);
    }
    builder.append("]*");
    return Pattern.compile(builder.toString());
  }
}
