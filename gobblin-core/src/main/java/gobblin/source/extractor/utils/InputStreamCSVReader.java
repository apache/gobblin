/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.utils;

import gobblin.configuration.ConfigurationKeys;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.nio.charset.Charset;
import java.util.ArrayList;


/**
 * Reads data from inputStream or bufferedReader and gives records as a list
 *
 * @author nveeramr
 */

public class InputStreamCSVReader {
  private StreamTokenizer parser;
  private char separator;
  private int maxFieldCount;
  boolean atEOF;

  public InputStreamCSVReader(Reader input) {
    this(new BufferedReader(input));
  }

  public InputStreamCSVReader(InputStream input) {
    this(new InputStreamReader(input, Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));
  }

  public InputStreamCSVReader(BufferedReader input) {
    this(input, ',', '\"');
  }

  public InputStreamCSVReader(String input) {
    this(new InputStreamReader(new ByteArrayInputStream(input.getBytes()),
        Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)), ',', '\"');
  }

  public InputStreamCSVReader(Reader input, char customizedSeparator) {
    this(new BufferedReader(input), customizedSeparator, '\"');
  }

  public InputStreamCSVReader(InputStream input, char customizedSeparator) {
    this(new InputStreamReader(input, Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)), customizedSeparator,
        '\"');
  }

  public InputStreamCSVReader(BufferedReader input, char customizedSeparator) {
    this(input, customizedSeparator, '\"');
  }

  public InputStreamCSVReader(String input, char customizedSeparator) {
    this(new InputStreamReader(new ByteArrayInputStream(input.getBytes()),
        Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)), customizedSeparator, '\"');
  }

  public InputStreamCSVReader(Reader input, char customizedSeparator, char enclosedChar) {
    this(new BufferedReader(input), customizedSeparator, enclosedChar);
  }

  public InputStreamCSVReader(InputStream input, char customizedSeparator, char enclosedChar) {
    this(new InputStreamReader(input, Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)), customizedSeparator,
        enclosedChar);
  }

  public InputStreamCSVReader(String input, char customizedSeparator, char enclosedChar) {
    this(new InputStreamReader(new ByteArrayInputStream(input.getBytes()),
        Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)), customizedSeparator, enclosedChar);
  }

  public InputStreamCSVReader(BufferedReader input, char separator, char enclosedChar) {
    this.separator = separator;
    // parser settings for the separator and escape chars
    parser = new StreamTokenizer(input);
    parser.ordinaryChars(0, 255);
    parser.wordChars(0, 255);
    parser.ordinaryChar(enclosedChar);
    parser.ordinaryChar(separator);
    parser.eolIsSignificant(true);
    parser.whitespaceChars('\n', '\n');
    parser.whitespaceChars('\r', '\r');
    atEOF = false;
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

  private ArrayList<String> getNextRecordFromStream()
      throws IOException {
    if (atEOF) {
      return null;
    }

    ArrayList<String> record = new ArrayList<String>(maxFieldCount);

    StringBuilder fieldValue = null;

    while (true) {
      int token = parser.nextToken();

      if (token == StreamTokenizer.TT_EOF) {
        addField(record, fieldValue);
        atEOF = true;
        break;
      }

      if (token == StreamTokenizer.TT_EOL) {
        addField(record, fieldValue);
        break;
      }

      if (token == separator) {
        addField(record, fieldValue);
        fieldValue = null;
        continue;
      }

      if (token == StreamTokenizer.TT_WORD) {
        if (fieldValue != null) {
          throw new CSVParseException("Unknown error", parser.lineno());
        }

        fieldValue = new StringBuilder(parser.sval);
        continue;
      }

      if (token == '"') {
        if (fieldValue != null) {
          throw new CSVParseException("Found unescaped quote. A value with quote should be within a quote",
              parser.lineno());
        }

        while (true) {
          token = parser.nextToken();

          if (token == StreamTokenizer.TT_EOF) {
            atEOF = true;
            throw new CSVParseException("EOF reached before closing an opened quote", parser.lineno());
          }

          if (token == separator) {
            fieldValue = appendFieldValue(fieldValue, token);
            continue;
          }

          if (token == StreamTokenizer.TT_EOL) {
            fieldValue = appendFieldValue(fieldValue, "\n");
            continue;
          }

          if (token == StreamTokenizer.TT_WORD) {
            fieldValue = appendFieldValue(fieldValue, parser.sval);
            continue;
          }

          if (token == '"') {
            int nextToken = parser.nextToken();

            if (nextToken == '"') {
              fieldValue = appendFieldValue(fieldValue, nextToken);
              continue;
            }

            if (nextToken == StreamTokenizer.TT_WORD) {
              throw new CSVParseException("Not expecting more text after end quote", parser.lineno());
            } else {
              parser.pushBack();
              break;
            }
          }
        }
      }
    }

    if (record.size() > maxFieldCount) {
      maxFieldCount = record.size();
    }

    return record;
  }

  private StringBuilder appendFieldValue(StringBuilder fieldValue, int token)
      throws CSVParseException {
    return appendFieldValue(fieldValue, "" + (char) token);
  }

  private StringBuilder appendFieldValue(StringBuilder fieldValue, String token)
      throws CSVParseException {
    if (fieldValue == null) {
      fieldValue = new StringBuilder();
    }

    fieldValue.append(token);
    return fieldValue;
  }

  private void addField(ArrayList<String> record, StringBuilder fieldValue)
      throws CSVParseException {
    record.add(fieldValue == null ? null : fieldValue.toString());
  }

  public static class CSVParseException extends IOException {
    final int recordNumber;

    CSVParseException(String message, int lineno) {
      super(message);
      recordNumber = lineno;
    }

    CSVParseException(int i) {
      recordNumber = i;
    }

    public int getRecordNumber() {
      return recordNumber;
    }
  }
}
