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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/** Test {@link FsScalingDirectivesRecipient} */
public class FsScalingDirectivesRecipientTest {

  @Mock
  private FileSystem fileSystem;
  private FsScalingDirectivesRecipient recipient;
  private ScalingDirectiveParser sdParser = new ScalingDirectiveParser();
  private static final String DIRECTIVES_DIR = "/test/dynamic/directives";

  @BeforeMethod
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    recipient = new FsScalingDirectivesRecipient(fileSystem, DIRECTIVES_DIR);
  }

  @Test
  public void testReceiveWithShortDirectives() throws IOException {
    List<String> directivesStrs = Arrays.asList(
        "1700010000.=4",
        "1700020000.new_profile=7",
        "1700030000.new_profile=7,+(a.b.c=7, x.y=five)"
    );

    FSDataOutputStream mockOutputStream = Mockito.mock(FSDataOutputStream.class);
    Mockito.when(fileSystem.create(Mockito.any(Path.class), Mockito.eq(false))).thenReturn(mockOutputStream);

    recipient.receive(directivesStrs.stream().map(str -> {
      try {
        return sdParser.parse(str);
      } catch (ScalingDirectiveParser.InvalidSyntaxException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList()));

    Mockito.verify(fileSystem).mkdirs(Mockito.eq(new Path(DIRECTIVES_DIR)));
    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    Mockito.verify(fileSystem, Mockito.times(directivesStrs.size())).create(pathCaptor.capture(), Mockito.eq(false));
    List<Path> capturedPaths = pathCaptor.getAllValues();

    Assert.assertEquals(capturedPaths.get(0), new Path(DIRECTIVES_DIR, directivesStrs.get(0)));
    Assert.assertEquals(capturedPaths.get(1), new Path(DIRECTIVES_DIR, directivesStrs.get(1)));
    Assert.assertEquals(capturedPaths.get(2), new Path(DIRECTIVES_DIR, directivesStrs.get(2)));
    Mockito.verifyNoMoreInteractions(fileSystem);
  }

  @Test
  public void testReceiveWithLongDirective() throws IOException {
    String profileName = "derivedProfile";
    int setPoint = 42;
    long timestamp = 1234567890;
    String basisProfileName = "origProfile";
    // NOTE: 42 chars to render the above + 1 for the closing `)`... 212 remaining

    String alphabet = IntStream.rangeClosed('a', 'z').collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    List<String> removeKeys = Arrays.asList(
        alphabet,                        // len = 26
        alphabet.toUpperCase(),
        alphabet.substring(1), // len = 25
        alphabet.substring(1).toUpperCase(),
        alphabet.substring(2), // len = 24
        alphabet.substring(2).toUpperCase(),
        alphabet.substring(3), // len = 23
        alphabet.substring(3).toUpperCase(),
        alphabet.substring(4)  // len = 22
    );
    ScalingDirective nearlyALongDirective = new ScalingDirective(profileName, setPoint, timestamp, basisProfileName,
        new ProfileOverlay.Removing(removeKeys.subList(0, removeKeys.size() - 2)));
    ScalingDirective aLongDirective = new ScalingDirective(profileName, setPoint, timestamp, basisProfileName,
        new ProfileOverlay.Removing(removeKeys));

    String nearlyALongDirectiveForm = ScalingDirectiveParser.asString(nearlyALongDirective);
    ScalingDirectiveParser.StringWithPlaceholderPlusOverlay aLongDirectiveForm = ScalingDirectiveParser.asStringWithPlaceholderPlusOverlay(aLongDirective);
    Assert.assertTrue(aLongDirectiveForm.getOverlayDefinitionString().length() > 0);

    FSDataOutputStream mockOutputStream = Mockito.mock(FSDataOutputStream.class);
    Mockito.when(fileSystem.create(Mockito.any(Path.class), Mockito.eq(false))).thenReturn(mockOutputStream);

    recipient.receive(Arrays.asList(nearlyALongDirective, aLongDirective));

    Mockito.verify(fileSystem).mkdirs(Mockito.eq(new Path(DIRECTIVES_DIR)));
    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    Mockito.verify(fileSystem, Mockito.times(2)).create(pathCaptor.capture(), Mockito.eq(false));
    List<Path> capturedPaths = pathCaptor.getAllValues();
    Assert.assertEquals(capturedPaths.get(0), new Path(DIRECTIVES_DIR, nearlyALongDirectiveForm));
    Assert.assertEquals(capturedPaths.get(1), new Path(DIRECTIVES_DIR, aLongDirectiveForm.getDirectiveStringWithPlaceholder()));
    Mockito.verifyNoMoreInteractions(fileSystem);

    Mockito.verify(mockOutputStream).writeUTF(Mockito.eq(aLongDirectiveForm.getOverlayDefinitionString()));
    Mockito.verify(mockOutputStream, Mockito.times(2)).close();
    Mockito.verifyNoMoreInteractions(mockOutputStream);
  }
}
