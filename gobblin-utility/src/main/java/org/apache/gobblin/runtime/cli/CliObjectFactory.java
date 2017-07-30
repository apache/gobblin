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

package gobblin.runtime.cli;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;


/**
 * A factory used to create an object from CLI arguments backed by commons-cli library.
 */
public interface CliObjectFactory<T> {
  /**
   * @return an instance of {@link Options} understood by this factory.
   */
  Options getOptions();

  /**
   * Build an instance of T from the parsed {@link CommandLine}. The input {@link CommandLine} was parsed with
   * the output of {@link Options}, but may include additional {@link org.apache.commons.cli.Option}s added by the driver.
   */
   T buildObject(CommandLine cli);

  /**
   * Build an instance of T from the input arguments.
   * @param args input arguments to the application.
   * @param offset will only start processing from this argument number.
   * @param printUsage if true, a failure or -h will cause help to be printed and an exception to be thrown.
   * @param usage usage String to be printed at the beginning of help message.
   */
   T buildObject(String[] args, int offset, boolean printUsage, String usage) throws IOException;

  /**
   * Get a usage string for display on the command line. The output of this method will be appended to the base string
   * "gobblin run <appName>". This should specify required options or parameters.
   */
  String getUsageString();

  /**
   * Thrown if help argument (-h) was found in the app arguments.
   */
  public static class HelpArgumentFound extends RuntimeException {
  }
}
