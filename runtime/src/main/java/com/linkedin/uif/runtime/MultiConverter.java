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

package com.linkedin.uif.runtime;

import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.converter.SingleRecordIterable;


/**
 * An implementation of {@link Converter} that applies a given list of {@link Converter}s in the given order.
 *
 * @author ynli
 */
@SuppressWarnings("unchecked")
public class MultiConverter extends Converter<Object, Object, Object, Object> {

  // The list of converters to be applied
  private final List<Converter<?, ?, ?, ?>> converters;
  // Remember the mapping between converter and schema it generates
  private final Map<Converter<?, ?, ?, ?>, Object> convertedSchemaMap = Maps.newHashMap();

  public MultiConverter(List<Converter<?, ?, ?, ?>> converters) {
    // Make a copy to guard against changes to the converters from outside
    this.converters = Lists.newArrayList(converters);
  }

  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {

    Object schema = inputSchema;
    for (Converter converter : this.converters) {
      // Apply the converter and remember the output schema of this converter
      schema = converter.convertSchema(schema, workUnit);
      this.convertedSchemaMap.put(converter, schema);
    }

    return schema;
  }

  @Override
  public Iterable<Object> convertRecord(Object outputSchema, final Object inputRecord, final WorkUnitState workUnit)
      throws DataConversionException {

    if (this.convertedSchemaMap.size() != this.converters.size()) {
      throw new RuntimeException("convertRecord should be called only after convertSchema is called");
    }

    return new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        try {
          return new MultiConverterIterator(inputRecord, workUnit);
        } catch (DataConversionException dce) {
          throw new RuntimeException(dce);
        }
      }
    };
  }

  /**
   * A type of {@link java.util.Iterator} to be used with {@link MultiConverter}.
   */
  private class MultiConverterIterator implements Iterator<Object> {

    private final WorkUnitState workUnitState;
    private final Deque<Iterator<Object>> converterIteratorStack = Lists.newLinkedList();
    private Iterator<Object> outputIterator;

    public MultiConverterIterator(Object inputRecord, WorkUnitState workUnitState) throws DataConversionException {
      this.workUnitState = workUnitState;

      // Construct the initial stack of converter iterators
      if (converters.isEmpty()) {
        this.converterIteratorStack.push(new SingleRecordIterable<Object>(inputRecord).iterator());
      } else {
        Object record = inputRecord;
        for (Converter converter : converters) {
          if (!this.converterIteratorStack.isEmpty()) {
            record = this.converterIteratorStack.peek().next();
          }
          Iterator<Object> iterator =
              converter.convertRecord(convertedSchemaMap.get(converter), record, workUnitState).iterator();
          this.converterIteratorStack.push(iterator);
          // Do not continue when encountering an empty iterator
          if (!iterator.hasNext()) {
            break;
          }
        }
      }

      this.outputIterator = this.converterIteratorStack.peek();
    }

    @Override
    public boolean hasNext() {
      if (this.outputIterator.hasNext()) {
        return true;
      }

      // Pop out all empty converter iterators from the stack
      while (!this.converterIteratorStack.isEmpty() && !this.converterIteratorStack.peek().hasNext()) {
        this.converterIteratorStack.pop();
      }

      if (this.converterIteratorStack.isEmpty()) {
        return false;
      }

      // Get the next record from the iterator (that still has more records) at the top of the stack
      // and reconstruct the stack of converter iterators from that record
      for (int i = this.converterIteratorStack.size(); i < converters.size(); i++) {
        Object record = this.converterIteratorStack.peek().next();
        Converter converter = converters.get(i);
        try {
          Iterator<Object> iterator =
              converter.convertRecord(convertedSchemaMap.get(converter), record, this.workUnitState).iterator();
          this.converterIteratorStack.push(iterator);
          // Do not continue when encountering an empty iterator
          if (!iterator.hasNext()) {
            break;
          }
        } catch (DataConversionException dce) {
          throw new RuntimeException(dce);
        }
      }

      this.outputIterator = this.converterIteratorStack.peek();
      return this.outputIterator.hasNext();
    }

    @Override
    public Object next() {
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      }
      return this.outputIterator.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
