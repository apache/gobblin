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

package org.apache.gobblin.runtime.cli;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.util.CLIPasswordEncryptor;


/**
 * An application that uses {@link org.apache.gobblin.password.PasswordManager} to encrypt and decrypt strings.
 */
@Alias(value = "passwordManager", description = "Encrypt or decrypt strings for the password manager.")
public class PasswordManagerCLI implements CliApplication {

  @Override
  public void run(String[] args) {
    try {
      CLIPasswordEncryptor.main(args);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
