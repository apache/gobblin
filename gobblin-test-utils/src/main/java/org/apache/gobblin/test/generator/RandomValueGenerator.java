package org.apache.gobblin.test.generator;

import java.util.Random;
import java.util.function.Supplier;

import lombok.Builder;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(
    name="random",
    targetTypes = {Type.Boolean, Type.Integer, Type.Long, Type.String, Type.Enum},
    configuration = RandomValueGeneratorConfig.class)
public class RandomValueGenerator<T> implements ValueGenerator<T> {


  private class RandomGenerator<T> implements Supplier<T> {
    private final Random rng;
    private final T[] interestingValues;
    private final Supplier<T> defaultSupplier;

    public RandomGenerator(Random rng, T[] interestingValues, Supplier<T> defaultSupplier) {
      this.rng = rng;
      this.interestingValues = interestingValues;
      this.defaultSupplier = defaultSupplier;
    }

    @Override
    public T get() {
      int choice = rng.nextInt(20);
      if (choice < interestingValues.length) {
        return interestingValues[choice];
      } else {
        return defaultSupplier.get();
      }
    }
  }

  private final Type logicalType;
  private final long seed;
  private final Supplier configuredGenerator;

  @Builder
  public RandomValueGenerator(RandomValueGeneratorConfig config) {
    config.getFieldConfig().validate();
    this.logicalType = config.getFieldConfig().getType();
    this.seed = config.getSeed();
    Random random = new Random(this.seed);
    switch (this.logicalType) {
      case Integer: {
        Integer[] interestingValues = {Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        configuredGenerator = new RandomGenerator(random, interestingValues, random::nextInt);
        break;
      }
      case Long: {
        Long[] interestingValues = { Long.MAX_VALUE, Long.MIN_VALUE, 0L};
        configuredGenerator = new RandomGenerator(random, interestingValues, random::nextLong);
        break;
      }
      case String: {
        String[] interestingValues = {"", "null"};
        configuredGenerator = new RandomGenerator(random, interestingValues,
            () -> TestUtils.generateRandomAlphaString(20));
        break;
      }
      case Boolean: {
        configuredGenerator = random::nextBoolean;
        break;
      }
      case Bytes: {
        configuredGenerator = () -> TestUtils.generateRandomBytes(20);
        break;
      }
      case Enum: {
        int symbolsLength = config.getFieldConfig().getSymbols().size();
        configuredGenerator = () -> config.getFieldConfig().getSymbols().get(
            TestUtils.generateRandomInteger(symbolsLength));
        break;
      }
      default:
        throw new RuntimeException("RandomValueGenerator does not support type " + this.logicalType.toString());
    }
  }

  @Override
  public Type getLogicalType() {
    return this.logicalType;
  }

  @Override
  public T get() {
    return (T) this.configuredGenerator.get();
  }
}
