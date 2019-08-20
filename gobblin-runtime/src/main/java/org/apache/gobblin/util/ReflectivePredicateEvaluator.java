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

package org.apache.gobblin.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import lombok.Data;


/**
 * An predicate evaluator that uses an interface to define a table schema and can evaluate SQL statements on instances of
 * that interface. See {@link ReflectivePredicateEvaluatorTest} for examples.
 *
 * Note all evaluated statements must return a single row with a single boolean column.
 *
 * Usage:
 * ReflectivePredicateEvaluator<MyInterface> evaluator = new ReflectivePredicateEvaluator<>(MyInterface.class, "SELECT ... FROM myInterface");
 * evaluator.evaluate(instance1, instance2, ...); // use the statement provided in constructor
 * -- or --
 * evaluator.evaluate("SELECT ... FROM myInterface", instance1, instance2, ...);
 */
public class ReflectivePredicateEvaluator implements Closeable {
	private static final String REFERENCE_INTERFACES = "refInterface";
	private static final String OPERATOR_ID = "operatorId";
	private static final Pattern FIELD_NAME_EXTRACTOR = Pattern.compile("(?:get([A-Z]))?(.+)");

	private static final String MODEL_PATTERN = "{"
			+ "version: 1, defaultSchema: 'MAIN',"
			+ "schemas: ["
			+ "{name: 'MAIN', type: 'custom', factory: '%s', operand: {%s: '%s', %s: '%d'}}"
			+ "]}";
	private static final String CONNECT_STRING_PATTERN = "jdbc:calcite:model=inline:%s";

	private static final Cache<Integer, ReflectivePredicateEvaluator> REGISTRY = CacheBuilder.newBuilder().weakValues().build();
	private static final AtomicInteger IDENTIFIER = new AtomicInteger();

	private final List<Class<?>> referenceInterfaces;
	private final int identifier;
	private final Connection conn;
	private final PreparedStatement stmnt;

	private final String sql;

	private volatile List<Object> objects;

	/**
	 * @param sql The default SQL expression to run in this evaluator.
	 * @param referenceInterfaces The interface that will be used to generate the table schema.
	 * @throws SQLException
	 */
	public ReflectivePredicateEvaluator(String sql, Class<?>... referenceInterfaces) throws SQLException  {
		this.referenceInterfaces = Lists.newArrayList(referenceInterfaces);
		this.sql = sql;

		this.identifier = IDENTIFIER.getAndIncrement();
		REGISTRY.put(this.identifier, this);

		String model = computeModel();
		String connectString = String.format(CONNECT_STRING_PATTERN, model);

		this.conn =
				DriverManager.getConnection(connectString);
		this.stmnt = prepareStatement(sql);
	}

	private PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement stmnt = null;
		try {
			stmnt = this.conn.prepareStatement(sql);
			validateSql(stmnt, sql);
			return stmnt;
		} catch (Throwable t) {
			if (stmnt != null) {
				stmnt.close();
			}
			throw t;
		}
	}

	private String computeModel() {
		return String.format(MODEL_PATTERN, PESchemaFactory.class.getName(), REFERENCE_INTERFACES,
				Joiner.on(",").join(this.referenceInterfaces.stream().map(Class::getName).collect(Collectors.toList())),
				OPERATOR_ID, this.identifier);
	}

	private void validateSql(PreparedStatement stmnt, String sql) throws SQLException {
		ResultSetMetaData metaData = stmnt.getMetaData();

		if (metaData.getColumnCount() != 1 || metaData.getColumnType(1) != Types.BOOLEAN) {
			throw new IllegalArgumentException("Statement is expected to return a single boolean column. Provided statement: " + sql);
		}
	}

	/**
	 * Evaluate the default predicate on the list of provided objects.
	 * @throws SQLException
	 */
	public boolean evaluate(Object... objects) throws SQLException{
		return evaluate(Lists.newArrayList(objects), null);
	}

	/**
	 * Evaluate an ad-hoc predicate on the list of provided objects.
	 * Note {@link #evaluate(Object...)} is preferable as it only does validation of the expression once.
	 * @throws SQLException
	 */
	public boolean evaluate(String sql, Object... objects) throws SQLException{
		return evaluate(Lists.newArrayList(objects), sql);
	}

	/**
	 * Evaluate the default predicate on the list of provided objects.
	 * @throws SQLException
	 */
	public boolean evaluate(List<Object> objects) throws SQLException {
		return evaluate(objects, null);
	}

	/**
	 * Evaluate an ad-hoc predicate on the list of provided objects.
	 * Note {@link #evaluate(Object[])} is preferable as it only does validation of the expression once.
	 * @throws SQLException
	 */
	public boolean evaluate(List<Object> objects, String sql) throws SQLException {
		synchronized (this) {
			String actualSql = sql == null ? this.sql : sql;
			PreparedStatement actualStmnt = null;
			try {
				actualStmnt = sql == null ? this.stmnt : prepareStatement(sql);

				this.objects = objects;
				actualStmnt.execute();
				ResultSet rs = actualStmnt.getResultSet();
				if (!rs.next()) {
					throw new IllegalArgumentException("Expected at least one returned row. SQL evaluated: " + actualSql);
				}
				boolean result = true;
				do {
					result &= rs.getBoolean(1);
				} while (rs.next());
				return result;
			} finally {
				if (sql != null && actualStmnt != null) {
					actualStmnt.close();
				}
			}
		}
	}

	@Override
	public void close()
			throws IOException {
		try {
			if (this.stmnt != null) {
				this.stmnt.close();
			}
			if (this.conn != null) {
				this.conn.close();
			}
		} catch (SQLException exc) {
			throw new IOException("Failed to close " + ReflectivePredicateEvaluator.class.getSimpleName(), exc);
		}
	}

	/**
	 * Calcite {@link SchemaFactory} used for the evaluator.
	 * This class is public because Calcite uses reflection to instantiate it, there is no reason to use it anywhere else
	 * in Gobblin.
	 */
	public static class PESchemaFactory implements SchemaFactory {

		@Override
		public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
			try {
				List<Class<?>> referenceInterfaces = new ArrayList<>();
				for (String iface : Splitter.on(",").splitToList(operand.get(REFERENCE_INTERFACES).toString())) {
					referenceInterfaces.add(Class.forName(iface));
				}
				int operatorIdentifier = Integer.parseInt(operand.get(OPERATOR_ID).toString());

				return new AbstractSchema() {
					@Override
					protected Map<String, Table> getTableMap() {
						HashMap<String, Table> map = new HashMap<>();
						for (Class<?> iface : referenceInterfaces) {
							map.put(iface.getSimpleName().toUpperCase(),
									new PETable(iface, operatorIdentifier));
						}
						return map;
					}
				};
			} catch (ReflectiveOperationException roe) {
				throw new RuntimeException(roe);
			}
		}
	}

	@Data
	private static class PETable extends AbstractTable implements ProjectableFilterableTable {
		private final Class<?> referenceInterface;
		private final int operatorIdentifier;
		private volatile boolean initialized = false;

		private RelDataType rowType;
		private List<Function<Object, Object>> methodsForFields = new ArrayList<>();

		@Override
		public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
			List<Object> list = REGISTRY.getIfPresent(this.operatorIdentifier).objects;

			final int[] actualProjects = resolveProjects(projects);

			Enumerator<Object[]> enumerator =  Linq4j.enumerator(list.stream()
					.filter(o -> referenceInterface.isAssignableFrom(o.getClass()))
					.map(
					m -> {
						Object[] res = new Object[actualProjects.length];
						for (int i = 0; i < actualProjects.length; i++) {
							res[i] = methodsForFields.get(actualProjects[i]).apply(m);
						}
						return res;
					}
			).collect(Collectors.toList()));

			return new AbstractEnumerable<Object[]>() {
				@Override
				public Enumerator<Object[]> enumerator() {
					return enumerator;
				}
			};
		}

		private int[] resolveProjects(int[] projects) {
			if (projects == null) {
				projects = new int[methodsForFields.size()];
				for (int i = 0; i < projects.length; i++) {
					projects[i] = i;
				}
			}
			return projects;
		}

		@Override
		public RelDataType getRowType(RelDataTypeFactory typeFactory) {
			initialize((JavaTypeFactory) typeFactory);
			return this.rowType;
		}

		private synchronized void initialize(JavaTypeFactory typeFactory) {
			if (this.initialized) {
				return;
			}

			this.methodsForFields = new ArrayList<>();
			List<RelDataTypeField> fields = new ArrayList<>();

			for (Method method : this.referenceInterface.getMethods()) {
				if (method.getParameterCount() == 0) {
					String fieldName = computeFieldName(method.getName());
					if (fieldName != null) {
						this.methodsForFields.add(extractorForMethod(method));
						Class<?> retType = method.getReturnType();
						if (retType.isEnum()) {
							retType = String.class;
						}
						fields.add(new RelDataTypeFieldImpl(fieldName.toUpperCase(), fields.size(), typeFactory.createType(retType)));
					}
				}
			}

			this.rowType = new MyDataType(fields, referenceInterface);
			this.initialized = true;
		}

		private Function<Object, Object> extractorForMethod(Method method) {
			return o -> {
				try {
					Object ret = method.invoke(o);
					return method.getReturnType().isEnum() ? ret.toString() : ret;
				} catch (ReflectiveOperationException roe) {
					throw new RuntimeException(roe);
				}
			};
		}

	}

	private static class MyDataType extends RelDataTypeImpl {
		private final String typeString;

		public MyDataType(List<? extends RelDataTypeField> fieldList, Class<?> refInterface) {
			super(fieldList);
			this.typeString = refInterface.getName();
			computeDigest();
		}

		@Override
		protected void generateTypeString(StringBuilder sb, boolean withDetail) {
			sb.append(typeString);
		}
	}

	private static String computeFieldName(String methodName) {
		Matcher matcher = FIELD_NAME_EXTRACTOR.matcher(methodName);
		if (matcher.matches()) {
			return matcher.group(1) + matcher.group(2);
		}
		return null;
	}

}
