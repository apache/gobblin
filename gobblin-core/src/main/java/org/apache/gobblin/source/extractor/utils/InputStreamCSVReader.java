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

package org.apache.gobblin.source.extractor.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayList;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * Reads data from inputStream or bufferedReader and gives records as a list
 *
 * @author nveeramr
 */

public class InputStreamCSVReader {

  private final StreamTokenizer parser;
  private final char separator;
  private int maxFieldCount;
  private boolean atEOF;

  public InputStreamCSVReader(Reader input) {
    this(new BufferedReader(input));
  }

  public InputStreamCSVReader(InputStream input) {
    this(new InputStreamReader(input, ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
  }

  public InputStreamCSVReader(BufferedReader input) {
    this(input, ',', '\"');
  }

  public InputStreamCSVReader(String input) {
    this(new InputStreamReader(new ByteArrayInputStream(input.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)),
        ConfigurationKeys.DEFAULT_CHARSET_ENCODING), ',', '\"');
  }

  public InputStreamCSVReader(Reader input, char customizedSeparator) {
    this(new BufferedReader(input), customizedSeparator, '\"');
  }

  public InputStreamCSVReader(InputStream input, char customizedSeparator) {
    this(new InputStreamReader(input, ConfigurationKeys.DEFAULT_CHARSET_ENCODING), customizedSeparator, '\"');
  }

  public InputStreamCSVReader(BufferedReader input, char customizedSeparator) {
    this(input, customizedSeparator, '\"');
  }

  public InputStreamCSVReader(String input, char customizedSeparator) {
    this(new InputStreamReader(new ByteArrayInputStream(input.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)),
        ConfigurationKeys.DEFAULT_CHARSET_ENCODING), customizedSeparator, '\"');
  }

  public InputStreamCSVReader(Reader input, char customizedSeparator, char enclosedChar) {
    this(new BufferedReader(input), customizedSeparator, enclosedChar);
  }

  public InputStreamCSVReader(InputStream input, char customizedSeparator, char enclosedChar) {
    this(new InputStreamReader(input, ConfigurationKeys.DEFAULT_CHARSET_ENCODING), customizedSeparator, enclosedChar);
  }

  public InputStreamCSVReader(String input, char customizedSeparator, char enclosedChar) {
    this(new InputStreamReader(new ByteArrayInputStream(input.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)),
        ConfigurationKeys.DEFAULT_CHARSET_ENCODING), customizedSeparator, enclosedChar);
  }

  public InputStreamCSVReader(BufferedReader input, char separator, char enclosedChar) {
    this.separator = separator;
    // parser settings for the separator and escape chars
    this.parser = new StreamTokenizer(input);
    this.parser.ordinaryChars(0, 255);
    this.parser.wordChars(0, 255);
    this.parser.ordinaryChar(enclosedChar);
    this.parser.ordinaryChar(separator);
    this.parser.eolIsSignificant(true);
    this.parser.whitespaceChars('\n', '\n');
    this.parser.whitespaceChars('\r', '\r');
    this.atEOF = false;
  }

  public ArrayList<String> splitRecord() throws IOException {
    ArrayList<String> record = this.getNextRecordFromStream();
    return record;
  }

  public ArrayList<String> nextRecord() throws IOException {
    ArrayList<String> record = this.getNextRecordFromStream();

    // skip record if it is empty
    while (record != null) {
      boolean emptyLine = false;
      if (record.size() == 0) {
        emptyLine = true;
      } else if (record.size() == 1) {
        String val = record.get(0);
        if (val == null || val.length() == 0) {
          emptyLine = true;
        }
      }

      if (emptyLine) {
        record = getNextRecordFromStream();
      } else {
        break;
      }
    }

    return record;
  }

  private ArrayList<String> getNextRecordFromStream() throws IOException {
    if (this.atEOF) {
      return null;
    }

    ArrayList<String> record = new ArrayList<>(this.maxFieldCount);

    StringBuilder fieldValue = null;

    while (true) {
      int token = this.parser.nextToken();

      if (token == StreamTokenizer.TT_EOF) {
        addField(record, fieldValue);
        this.atEOF = true;
        break;
      }

      if (token == StreamTokenizer.TT_EOL) {
        addField(record, fieldValue);
        break;
      }

      if (token == this.separator) {
        addField(record, fieldValue);
        fieldValue = null;
        continue;
      }

      if (token == StreamTokenizer.TT_WORD) {
        if (fieldValue != null) {
          throw new CSVParseException("Unknown error", this.parser.lineno());
        }

        fieldValue = new StringBuilder(this.parser.sval);
        continue;
      }

      if (token == '"') {
        if (fieldValue != null) {
          throw new CSVParseException("Found unescaped quote. A value with quote should be within a quote",
              this.parser.lineno());
        }

        while (true) {
          token = this.parser.nextToken();

          if (token == StreamTokenizer.TT_EOF) {
            this.atEOF = true;
            throw new CSVParseException("EOF reached before closing an opened quote", this.parser.lineno());
          }

          if (token == this.separator) {
            fieldValue = appendFieldValue(fieldValue, token);
            continue;
          }

          if (token == StreamTokenizer.TT_EOL) {
            fieldValue = appendFieldValue(fieldValue, "\n");
            continue;
          }

          if (token == StreamTokenizer.TT_WORD) {
            fieldValue = appendFieldValue(fieldValue, this.parser.sval);
            continue;
          }

          if (token == '"') {
            int nextToken = this.parser.nextToken();

            if (nextToken == '"') {
              fieldValue = appendFieldValue(fieldValue, nextToken);
              continue;
            }

            if (nextToken == StreamTokenizer.TT_WORD) {
              throw new CSVParseException("Not expecting more text after end quote", this.parser.lineno());
            }
            this.parser.pushBack();
            break;
          }
        }
      }
    }

    if (record.size() > this.maxFieldCount) {
      this.maxFieldCount = record.size();
    }

    return record;
  }

  private static StringBuilder appendFieldValue(StringBuilder fieldValue, int token) {
    return appendFieldValue(fieldValue, "" + (char) token);
  }

  private static StringBuilder appendFieldValue(StringBuilder fieldValue, String token) {
    if (fieldValue == null) {
      fieldValue = new StringBuilder();
    }

    fieldValue.append(token);
    return fieldValue;
  }

  private static void addField(ArrayList<String> record, StringBuilder fieldValue) {
    record.add(fieldValue == null ? null : fieldValue.toString());
  }

  public static class CSVParseException extends IOException {
    private static final long serialVersionUID = 1L;
    final int recordNumber;

    CSVParseException(String message, int lineno) {
      super(message);
      this.recordNumber = lineno;
    }

    CSVParseException(int i) {
      this.recordNumber = i;
    }

    public int getRecordNumber() {
      return this.recordNumber;
    }
  }
}
