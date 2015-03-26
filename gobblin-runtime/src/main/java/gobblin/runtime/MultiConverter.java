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

import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


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
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {

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

    final List<Converter<?, ?, ?, ?>> converters = this.converters;

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
   * A type of {@link java.util.Iterator} to be used with {@link MultiConverter}. This method works by maintaining a
   * list of {@link ConverterIteratorPair} elements. There is an entry in the list for each converter present. The code
   * iterates up and down the list by maintaining an index variable. It is necessary to move up and down the list
   * because at any moment a call to {@link Converter#convertRecord(Object, Object, WorkUnitState)} can return an
   * {@link EmptyIterable()}, meaning the iteration needs to fall back to a previous element.
   */
  private class MultiConverterIterator implements Iterator<Object> {

    private final WorkUnitState workUnitState;

    // A LinkedList of Iterator<Object>, where each iterator is produced by a converter
    private final Deque<Iterator<Object>> converterIteratorStack = Lists.newLinkedList();

    // currentRecord contains either the next element to be returned, or the element highest on converterIteratorStack
    private Object currentRecord;

    public MultiConverterIterator(Object inputRecord, WorkUnitState workUnitState) throws DataConversionException {
      this.workUnitState = workUnitState;
      this.currentRecord = inputRecord;

      // Set the new value of currentRecord
      setNextRecord();
    }

    /**
     * Helper method to set the newest value of currentRecord, after this method is called then it is safe to return the
     * value of currentRecord. THe method works by propagating the value currentRecord up the list until all converters
     * have been reset based on the value of currentRecord. If the propagation ever finds an EmptyIterable, then it
     * moves to the next available element in the list.
     * @throws DataConversionException
     */
    public void setNextRecord() throws DataConversionException {
      while (this.currentRecord != null && this.converterIteratorStack.size() != converters.size()) {
        Converter converter = converters.get(converterIteratorStack.size());
        Iterator<Object> iterator =
            converter.convertRecord(convertedSchemaMap.get(converter), this.currentRecord, this.workUnitState)
                .iterator();

        if (iterator.hasNext()) {
          this.converterIteratorStack.push(iterator);
          this.currentRecord = iterator.next();

        } else {
          this.currentRecord = getNextElementFromList();
        }
      }
    }

    /**
     * Helper method to get the next element on the list. This does a search down the list from index to element 0,
     * searching each ConverterIteratorPair to see if any iterator contains an element. Once it finds that element, it
     * returns it.
     * @return
     */
    public Object getNextElementFromList() {
      Iterator<Object> itr;

      while (!this.converterIteratorStack.isEmpty()) {
        itr = this.converterIteratorStack.peek();
        if (itr.hasNext()) {
          return itr.next();
        } else {
          this.converterIteratorStack.pop();
        }
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      return this.currentRecord != null;
    }

    @Override
    public Object next() {

      // The returnRecord will be the record that is returned by this method
      Object returnRecord = this.currentRecord;

      // Move the currentRecord point to the next record, or null if there are no more records left
      if (this.converterIteratorStack.isEmpty()) {
        this.currentRecord = null;
        return returnRecord;
      }

      // If the top of the stack has data, then the next currentRecord will come from that iterator
      Iterator<Object> lastItr = this.converterIteratorStack.peek();
      if (lastItr.hasNext()) {
        this.currentRecord = lastItr.next();

      } else {

        // Get the next element from the stack
        this.currentRecord = getNextElementFromList();

        // If there are no more elements in the stack, then return returnRecord
        if (this.currentRecord == null) {
          return returnRecord;
        }

        // Set the new value of currentRecord
        try {
          setNextRecord();
        } catch (DataConversionException e) {
          Throwables.propagate(e);
        }
      }

      return returnRecord;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
