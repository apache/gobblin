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
package org.apache.gobblin.crypto;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.KeyStoreException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.xml.bind.DatatypeConverter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.crypto.JCEKSKeystoreCredentialStore;
import org.apache.gobblin.runtime.cli.CliApplication;


@Alias(value = "keystore", description = "Examine JCE Keystore files")
@Slf4j
public class JCEKSKeystoreCredentialStoreCli implements CliApplication {
  private static final Map<String, Action> actionMap = ImmutableMap
      .of("generate_keys", new GenerateKeyAction(), "list_keys", new ListKeysAction(), "help", new HelpAction(),
          "export", new ExportKeyAction());

  @Override
  public void run(String[] args) {
    if (args.length < 2) {
      System.out.println("Must specify an action!");
      new HelpAction().run(args);
      return;
    }

    String actionStr = args[1];
    Action action = actionMap.get(actionStr);
    if (action == null) {
      System.out.println("Action " + actionStr + " unknown!");
      new HelpAction().run(args);
      return;
    }

    action.run(Arrays.copyOfRange(args, 1, args.length));
  }

  public static JCEKSKeystoreCredentialStore loadKeystore(String path)
      throws IOException {
    char[] password = getPasswordFromConsole();

    return new JCEKSKeystoreCredentialStore(path, String.valueOf(password));
  }

  /**
   * Abstract class for any action of this tool
   */
  static abstract class Action {
    /**
     * Return any additional Options for this action. The framework will always add a 'help' option.
     */
    protected abstract List<Option> getExtraOptions();

    /**
     * Execute the action
     * @param args
     */
    abstract void run(String[] args);

    protected static final Option HELP = Option.builder("h").longOpt("help").desc("Print usage").build();

    protected void printUsage() {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Options", getOptions());
    }

    /**
     * Helper function to parse CLI arguments
     */
    protected CommandLine parseOptions(String[] args)
        throws ParseException {
      CommandLineParser parser = new DefaultParser();
      return parser.parse(getOptions(), args);
    }

    private Options getOptions() {
      List<Option> options = getExtraOptions();
      Options optionList = new Options();
      optionList.addOption(HELP);
      for (Option o : options) {
        optionList.addOption(o);
      }

      return optionList;
    }
  }

  static class HelpAction extends Action {
    @Override
    protected List<Option> getExtraOptions() {
      return Collections.emptyList();
    }

    @Override
    void run(String[] args) {
      System.out.println("You can run <actionName> -h to see valid flags for a given action");
      for (String validAction : actionMap.keySet()) {
        System.out.println(validAction);
      }
    }
  }

  /**
   * Check how many keys are present in an existing keystore.
   */
  static class ListKeysAction extends Action {
    private static final Option KEYSTORE_LOCATION =
        Option.builder("o").longOpt("out").hasArg().desc("Keystore location").build();

    private static final List<Option> options = ImmutableList.of(KEYSTORE_LOCATION);

    @Override
    protected List<Option> getExtraOptions() {
      return options;
    }

    @Override
    void run(String[] args) {
      try {
        CommandLine cli = parseOptions(args);
        if (!paramsAreValid(cli)) {
          return;
        }

        String keystoreLocation = cli.getOptionValue(KEYSTORE_LOCATION.getOpt());
        JCEKSKeystoreCredentialStore credentialStore = loadKeystore(keystoreLocation);

        Map<String, byte[]> keys = credentialStore.getAllEncodedKeys();
        System.out.println("Keystore " + keystoreLocation + " has " + String.valueOf(keys.size()) + " keys.");
      } catch (IOException | ParseException e) {
        throw new RuntimeException(e);
      }
    }

    private boolean paramsAreValid(CommandLine cli) {
      if (cli.hasOption(HELP.getOpt())) {
        printUsage();
        return false;
      }

      if (!cli.hasOption(KEYSTORE_LOCATION.getOpt())) {
        System.out.println("Must specify keystore location!");
        printUsage();
        return false;
      }

      return true;
    }
  }

  /**
   * Create a new keystore file with _N_ serialized keys. The password will be read from the console.
   */
  static class GenerateKeyAction extends Action {

    private static final Option KEYSTORE_LOCATION =
        Option.builder("o").longOpt("out").hasArg().desc("Keystore location").build();
    private static final Option NUM_KEYS =
        Option.builder("n").longOpt("numKeys").hasArg().desc("# of keys to generate").build();

    private static final List<Option> OPTIONS = ImmutableList.of(KEYSTORE_LOCATION, NUM_KEYS);

    @Override
    protected List<Option> getExtraOptions() {
      return OPTIONS;
    }

    @Override
    void run(String[] args) {
      try {
        CommandLine cli = parseOptions(args);
        if (!paramsAreValid(cli)) {
          return;
        }

        int numKeys = Integer.parseInt(cli.getOptionValue(NUM_KEYS.getOpt(), "20"));

        char[] password = getPasswordFromConsole();
        String keystoreLocation = cli.getOptionValue(KEYSTORE_LOCATION.getOpt());
        JCEKSKeystoreCredentialStore credentialStore =
            new JCEKSKeystoreCredentialStore(cli.getOptionValue(KEYSTORE_LOCATION.getOpt()), String.valueOf(password),
                EnumSet.of(JCEKSKeystoreCredentialStore.CreationOptions.CREATE_IF_MISSING));

        credentialStore.generateAesKeys(numKeys, 0);
        System.out.println("Generated " + String.valueOf(numKeys) + " keys at " + keystoreLocation);
      } catch (IOException | KeyStoreException e) {
        throw new RuntimeException(e);
      } catch (ParseException e) {
        System.out.println("Unknown command line params " + e.toString());
        printUsage();
      }
    }

    private boolean paramsAreValid(CommandLine cli) {
      if (cli.hasOption(HELP.getOpt())) {
        printUsage();
        return false;
      }

      if (!cli.hasOption(KEYSTORE_LOCATION.getOpt())) {
        System.out.println("Must specify keystore location!");
        printUsage();
        return false;
      }

      return true;
    }
  }

  public static char[] getPasswordFromConsole() {
    System.out.print("Please enter the keystore password: ");
    return System.console().readPassword();
  }

  static class ExportKeyAction extends Action {
    private static final Option KEYSTORE_LOCATION =
        Option.builder("i").longOpt("in").hasArg().required().desc("Keystore location").build();
    private static final Option OUTPUT_LOCATION =
        Option.builder("o").longOpt("out").hasArg().required().desc("Output location").build();

    @Override
    protected List<Option> getExtraOptions() {
      return ImmutableList.of(KEYSTORE_LOCATION, OUTPUT_LOCATION);
    }

    @Override
    void run(String[] args) {
      try {
        CommandLine cli = parseOptions(args);
        JCEKSKeystoreCredentialStore credStore = loadKeystore(cli.getOptionValue(KEYSTORE_LOCATION.getOpt()));
        Map<Integer, String> base64Keys = new HashMap<>();

        Map<String, byte[]> keys = credStore.getAllEncodedKeys();
        for (Map.Entry<String, byte[]> e: keys.entrySet()) {
          base64Keys.put(Integer.valueOf(e.getKey()), DatatypeConverter.printBase64Binary(e.getValue()));
        }

        OutputStreamWriter fOs = new OutputStreamWriter(
            new FileOutputStream(new File(cli.getOptionValue(OUTPUT_LOCATION.getOpt()))),
            StandardCharsets.UTF_8);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        fOs.write(gson.toJson(base64Keys));
        fOs.flush();
        fOs.close();
      } catch (ParseException e) {
        printUsage();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
