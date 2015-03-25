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

package gobblin.runtime;

import java.util.Iterator;

import com.google.common.collect.Iterators;

import gobblin.converter.Converter;


/**
 * Wrapper class for a {@link Converter} and a {@link Iterator}
 */
public class ConverterIteratorPair {

  private Iterator itr = Iterators.emptyIterator();
  private Converter converter;

  private ConverterIteratorPair() {
  }

  public static ConverterIteratorPair newConverterIterator() {
    return new ConverterIteratorPair();
  }

  public ConverterIteratorPair withIterator(Iterator itr) {
    this.itr = itr;
    return this;
  }

  public ConverterIteratorPair withConverter(Converter converter) {
    this.converter = converter;
    return this;
  }

  public Converter getConverter() {
    return this.converter;
  }

  public Iterator getIterator() {
    return this.itr;
  }

  public void setIterator(Iterator itr) {
    this.itr = itr;
  }
}
