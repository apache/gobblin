package org.apache.gobblin.util;

import org.junit.Assert;
import org.testng.annotations.Test;

import lombok.Data;

public class ReflectivePredicateEvaluatorTest {

	@Test
	public void simpleTest() throws Exception {
		ReflectivePredicateEvaluator evaluator = new ReflectivePredicateEvaluator(
				"SELECT anInt = 1 FROM myInterface", MyInterface.class);

		Assert.assertTrue(evaluator.evaluate(new MyImplementation(1, "foo")));
		Assert.assertFalse(evaluator.evaluate(new MyImplementation(2, "foo")));

		Assert.assertTrue(evaluator.evaluate("SELECT anInt = 1 OR aString = 'foo' FROM myInterface",
				new MyImplementation(1, "bar")));
		Assert.assertTrue(evaluator.evaluate("SELECT anInt = 1 OR aString = 'foo' FROM myInterface",
				new MyImplementation(2, "foo")));
		Assert.assertFalse(evaluator.evaluate("SELECT anInt = 1 OR aString = 'foo' FROM myInterface",
				new MyImplementation(2, "bar")));
	}

	@Test
	public void testWithAggregations() throws Exception {
		ReflectivePredicateEvaluator evaluator = new ReflectivePredicateEvaluator(
				"SELECT sum(anInt) = 5 FROM myInterface", MyInterface.class);

		Assert.assertFalse(evaluator.evaluate(new MyImplementation(1, "foo")));
		Assert.assertTrue(evaluator.evaluate(new MyImplementation(1, "foo"), new MyImplementation(4, "foo")));
		Assert.assertFalse(evaluator.evaluate(new MyImplementation(2, "foo"), new MyImplementation(4, "foo")));
	}

	@Test
	public void testWithAggregationsAndFilter() throws Exception {
		ReflectivePredicateEvaluator evaluator = new ReflectivePredicateEvaluator(
				"SELECT sum(anInt) = 5 FROM myInterface WHERE aString = 'foo'", MyInterface.class);

		Assert.assertFalse(evaluator.evaluate(new MyImplementation(1, "foo")));
		Assert.assertTrue(evaluator.evaluate(new MyImplementation(1, "foo"), new MyImplementation(4, "foo"), new MyImplementation(4, "bar")));
		Assert.assertFalse(evaluator.evaluate(new MyImplementation(1, "foo"), new MyImplementation(4, "foo"), new MyImplementation(4, "foo")));
	}

	@Test
	public void testMultipleInterfaces() throws Exception {
		ReflectivePredicateEvaluator evaluator = new ReflectivePredicateEvaluator(
				"SELECT true = ALL (SELECT sum(anInt) = 2 AS satisfied FROM myInterface UNION SELECT sum(anInt) = 3 AS satisfied FROM myInterface2)",
				MyInterface.class, MyInterface2.class);
		Assert.assertFalse(evaluator.evaluate(new MyImplementation(2, "foo")));
		Assert.assertTrue(evaluator.evaluate(new MyImplementation(2, "foo"), new MyImplementation2(3)));
		Assert.assertTrue(evaluator.evaluate(new MyImplementation(1, "foo"), new MyImplementation2(3), new MyImplementation(1, "foo")));
	}

	@Test
	public void testMultipleOutputs() throws Exception {
		ReflectivePredicateEvaluator evaluator =
				new ReflectivePredicateEvaluator("SELECT anInt = 1 FROM myInterface", MyInterface.class);
		Assert.assertTrue(evaluator.evaluate(new MyImplementation(1, "bar"), new MyImplementation(1, "foo")));
		Assert.assertFalse(evaluator.evaluate(new MyImplementation(1, "bar"), new MyImplementation(2, "foo")));
	}

	@Test
	public void testInvalidSQL() throws Exception {
		try {
			ReflectivePredicateEvaluator evaluator =
					new ReflectivePredicateEvaluator("SELECT anInt FROM myInterface", MyInterface.class);
			Assert.fail();
		} catch (IllegalArgumentException exc) {
			// Expected
		}
	}

	@Test
	public void testNoOutputs() throws Exception {
		try {
			ReflectivePredicateEvaluator evaluator =
					new ReflectivePredicateEvaluator("SELECT anInt = 1 FROM myInterface WHERE aString = 'foo'",
							MyInterface.class);
			evaluator.evaluate(new MyImplementation(1, "bar"));
			Assert.fail();
		} catch (IllegalArgumentException exc) {
			// Expected
		}
	}

	private interface MyInterface {
		int getAnInt();
		String getAString();
	}

	@Data
	private static class MyImplementation implements MyInterface {
		private final int anInt;
		private final String aString;
	}

	private interface MyInterface2 {
		int getAnInt();
	}

	@Data
	private static class MyImplementation2 implements MyInterface2 {
		private final int anInt;
	}

}
