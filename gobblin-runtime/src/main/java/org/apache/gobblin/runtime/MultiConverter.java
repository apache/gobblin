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

package gobblin.runtime;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.Constructs;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.IdentityConverter;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.state.ConstructState;

import lombok.Getter;


/**
 * An implementation of {@link Converter} that applies a given list of {@link Converter}s in the given order.
 *
 * @author Yinan Li
 */
@SuppressWarnings("unchecked")
public class MultiConverter extends Converter<Object, Object, Object, Object> {

  // The list of converters to be applied
  @Getter
  private final List<Converter<?, ?, ?, ?>> converters;
  // Remember the mapping between converter and schema it generates
  private final Map<Converter<?, ?, ?, ?>, Object> convertedSchemaMap = Maps.newHashMap();

  public MultiConverter(List<Converter<?, ?, ?, ?>> converters) {
    // Make a copy to guard against changes to the converters from outside
    this.converters = Lists.newArrayList(converters);
  }

  @Override
  public void close() throws IOException {
    for (Converter<?, ?, ?, ?> converter : this.converters) {
      converter.close();
    }
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

  @Override
  public State getFinalState() {
    ConstructState state = new ConstructState(super.getFinalState());
    for (int i = 0; i < this.converters.size(); i++) {
      state.addConstructState(Constructs.CONVERTER, new ConstructState(this.converters.get(i).getFinalState()),
          Integer.toString(i));
    }
    return state;
  }

  /**
   * A type of {@link java.util.Iterator} to be used with {@link MultiConverter}. The Converter uses the
   * {@link ChainedConverterIterator} to chain iterators together. The first {@link ChainedConverterIterator} created
   * contains an iterator with only the inputRecord and the first converter in the converters list. Each subsequent
   * {@link ChainedConverterIterator} is created using the previous {@link ChainedConverterIterator} along with the next
   * converter in the converters list. By chaining the converters and iterators in this fashion, a reference to the last
   * {@link ChainedConverterIterator} will be sufficient to iterate through all the data.
   */
  private class MultiConverterIterator implements Iterator<Object> {

    private final WorkUnitState workUnitState;

    private Iterator<Object> chainedConverterIterator;

    public MultiConverterIterator(Object inputRecord, WorkUnitState workUnitState) throws DataConversionException {
      this.workUnitState = workUnitState;
      this.chainedConverterIterator = new ChainedConverterIterator(new SingleRecordIterable<>(inputRecord).iterator(),
          MultiConverter.this.converters.isEmpty() ? new IdentityConverter() : MultiConverter.this.converters.get(0));

      for (int i = 1; i < MultiConverter.this.converters.size(); i++) {
        this.chainedConverterIterator =
            new ChainedConverterIterator(this.chainedConverterIterator, MultiConverter.this.converters.get(i));
      }
    }

    @Override
    public boolean hasNext() {
      return this.chainedConverterIterator.hasNext();
    }

    @Override
    public Object next() {
      return this.chainedConverterIterator.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * A helper class that implements {@link Iterator}. It is constructed with a {@link Iterator} and a {@link Converter}.
     * The class iterates through the results of each converted record from prevIterator. It iterates through each
     * element in prevIterator, converts the result, and then stores the result in the currentIterator object. It
     * returns every element in currentIterator until it is empty, and then it gets the next element from prevIterator,
     * converts the object, and stores the result in currentIterator. This pattern continues until there are no more
     * elements left in prevIterator.
     */
    private class ChainedConverterIterator implements Iterator<Object> {

      private final Converter converter;
      private final Iterator<Object> prevIterator;

      private Iterator<Object> currentIterator;

      public ChainedConverterIterator(Iterator<Object> prevIterator, Converter converter)
          throws DataConversionException {
        this.converter = converter;
        this.prevIterator = prevIterator;

        if (this.prevIterator.hasNext()) {
          this.currentIterator = converter.convertRecord(MultiConverter.this.convertedSchemaMap.get(converter),
              this.prevIterator.next(), MultiConverterIterator.this.workUnitState).iterator();
        } else {
          this.currentIterator = new EmptyIterable<>().iterator();
        }
      }

      @Override
      public boolean hasNext() {
        if (this.currentIterator.hasNext()) {
          return true;
        }
        while (this.prevIterator.hasNext()) {
          try {
            this.currentIterator =
                this.converter.convertRecord(MultiConverter.this.convertedSchemaMap.get(this.converter),
                    this.prevIterator.next(), MultiConverterIterator.this.workUnitState).iterator();
          } catch (DataConversionException e) {
            Throwables.propagate(e);
          }
          if (this.currentIterator.hasNext()) {
            return true;
          }
        }
        return false;
      }

      @Override
      public Object next() {
        if (this.hasNext()) {
          return this.currentIterator.next();
        }
        throw new NoSuchElementException();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }
}
