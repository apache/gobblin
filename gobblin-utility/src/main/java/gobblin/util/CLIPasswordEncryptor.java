package gobblin.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.jasypt.util.text.StrongTextEncryptor;


/**
 * A command line tool for encrypting password.
 * Usage: -h print usage, -p plain password, -m master password.
 *
 * @author ziliu
 */
public class CLIPasswordEncryptor {

  private static final char HELP_OPTION = 'h';
  private static final char PLAIN_PWD_OPTION = 'p';
  private static final char MASTER_PWD_OPTION = 'm';

  private static Options commandLineOptions;

  public static void main(String[] args) throws ParseException {
    CommandLine cl = parseArgs(args);
    if (shouldPrintUsageAndExit(cl)) {
      printUsage();
      return;
    }
    String plain = cl.getOptionValue(PLAIN_PWD_OPTION);
    String masterPassword = cl.getOptionValue(MASTER_PWD_OPTION);
    StrongTextEncryptor encryptor = new StrongTextEncryptor();
    encryptor.setPassword(masterPassword);
    System.out.println("ENC(" + encryptor.encrypt(plain) + ")");
  }

  private static CommandLine parseArgs(String[] args) throws ParseException {
    commandLineOptions = getOptions();
    return new DefaultParser().parse(commandLineOptions, args);
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption(new Option("h", "print this message"));
    options.addOption(Option.builder(StringUtils.EMPTY + PLAIN_PWD_OPTION).argName("plain password").hasArg()
        .desc("plain password to be encrypted").build());
    options.addOption(Option.builder(StringUtils.EMPTY + MASTER_PWD_OPTION).argName("master password").hasArg()
        .desc("master password used to encrypt the plain password").build());
    return options;
  }

  private static boolean shouldPrintUsageAndExit(CommandLine cl) {
    if (cl.hasOption(HELP_OPTION)) {
      return true;
    }
    if (!cl.hasOption(PLAIN_PWD_OPTION) || !cl.hasOption(MASTER_PWD_OPTION)) {
      return true;
    }
    return false;
  }

  private static void printUsage() {
    new HelpFormatter().printHelp(" ", commandLineOptions);
  }

}
