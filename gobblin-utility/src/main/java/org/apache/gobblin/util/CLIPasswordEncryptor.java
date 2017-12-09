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

package org.apache.gobblin.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.jasypt.util.text.BasicTextEncryptor;
import org.jasypt.util.text.StrongTextEncryptor;
import org.jasypt.util.text.TextEncryptor;

import com.google.common.base.Optional;

import org.apache.gobblin.password.PasswordManager;


/**
 * A command line tool for encrypting password.
 * Usage: -h print usage, -p plain password, -m master password, -f master password file, -s use strong encryptor.
 *
 * @author Ziyang Liu
 */
public class CLIPasswordEncryptor {

  private static final char HELP_OPTION = 'h';
  private static final char PLAIN_PWD_OPTION = 'p';
  private static final char MASTER_PWD_OPTION = 'm';
  private static final char STRONG_ENCRYPTOR_OPTION = 's';
  private static final char MASTER_PWD_FILE_OPTION = 'f';
  private static final char ENCRYPTED_PWD_OPTION = 'e';

  private static final Pattern ENCRYPTED_PATTERN = Pattern.compile("ENC\\(([^)]+)\\)");

  private static final Options CLI_OPTIONS = new Options();

  public static void main(String[] args) throws ParseException {
    CommandLine cl = parseArgs(args);
    if (shouldPrintUsageAndExit(cl)) {
      printUsage();
      return;
    }
    String masterPassword = getMasterPassword(cl);
    TextEncryptor encryptor = getEncryptor(cl, masterPassword);

    if (cl.hasOption(ENCRYPTED_PWD_OPTION)) {
      Matcher matcher = ENCRYPTED_PATTERN.matcher(cl.getOptionValue(ENCRYPTED_PWD_OPTION));
      if (matcher.find()) {
        String encrypted = matcher.group(1);
        System.out.println(encryptor.decrypt(encrypted));
      } else {
        throw new RuntimeException("Input encrypted password does not match pattern \"ENC(...)\"");
      }
    } else if (cl.hasOption(PLAIN_PWD_OPTION)){
      System.out.println("ENC(" + encryptor.encrypt(cl.getOptionValue(PLAIN_PWD_OPTION)) + ")");
    } else {
      printUsage();
      throw new RuntimeException(String.format("Must provide -%s or -%s option.", PLAIN_PWD_OPTION, ENCRYPTED_PWD_OPTION));
    }
  }

  private static TextEncryptor getEncryptor(CommandLine cl, String masterPassword) {
    if (cl.hasOption(STRONG_ENCRYPTOR_OPTION)) {
      StrongTextEncryptor encryptor = new StrongTextEncryptor();
      encryptor.setPassword(masterPassword);
      return encryptor;
    } else {
      BasicTextEncryptor encryptor = new BasicTextEncryptor();
      encryptor.setPassword(masterPassword);
      return encryptor;
    }
  }

  private static String getMasterPassword(CommandLine cl) {
    if (cl.hasOption(MASTER_PWD_OPTION)) {
      if (cl.hasOption(MASTER_PWD_FILE_OPTION)) {
        System.out.println(String.format("both -%s and -%s are provided. Using -%s", MASTER_PWD_OPTION,
            MASTER_PWD_FILE_OPTION, MASTER_PWD_OPTION));
      }
      return cl.getOptionValue(MASTER_PWD_OPTION);
    }
    Path masterPwdLoc = new Path(cl.getOptionValue(MASTER_PWD_FILE_OPTION));
    Optional<String> masterPwd = PasswordManager.getMasterPassword(masterPwdLoc);
    if (masterPwd.isPresent()) {
      return masterPwd.get();
    }
    throw new RuntimeException("Failed to get master password from " + masterPwdLoc);
  }

  private static CommandLine parseArgs(String[] args) throws ParseException {
    initOptions(CLI_OPTIONS);
    return new DefaultParser().parse(CLI_OPTIONS, args);
  }

  private static Options initOptions(Options options) {
    options.addOption(new Option(StringUtils.EMPTY + HELP_OPTION, "print this message"));
    options.addOption(Option.builder(StringUtils.EMPTY + PLAIN_PWD_OPTION).argName("plain password").hasArg()
        .desc("plain password to be encrypted").build());
    options.addOption(Option.builder(StringUtils.EMPTY + MASTER_PWD_OPTION).argName("master password").hasArg()
        .desc("master password used to encrypt the plain password").build());

    options.addOption(Option.builder(StringUtils.EMPTY + MASTER_PWD_FILE_OPTION).argName("master password file")
        .hasArg().desc("file that contains the master password used to encrypt the plain password").build());
    options.addOption(new Option(StringUtils.EMPTY + STRONG_ENCRYPTOR_OPTION, "use strong encryptor"));
    options.addOption(Option.builder(StringUtils.EMPTY + ENCRYPTED_PWD_OPTION).argName("decrypt the input").hasArg().build());
    return options;
  }

  private static boolean shouldPrintUsageAndExit(CommandLine cl) {
    if (cl.hasOption(HELP_OPTION)) {
      return true;
    }
    if (!masterpasswordProvided(cl)) {
      return true;
    }
    return false;
  }

  private static boolean masterpasswordProvided(CommandLine cl) {
    return cl.hasOption(MASTER_PWD_OPTION) || cl.hasOption(MASTER_PWD_FILE_OPTION);
  }

  private static void printUsage() {
    new HelpFormatter().printHelp(" ", CLI_OPTIONS);
  }

}
