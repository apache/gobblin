package org.apache.gobblin.test.generator.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.FieldConfigWithValueGenerator;
import org.apache.gobblin.test.generator.FixedStructValueGenerator;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.generator.config.FixedSchemaGeneratorConfig;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(
    name="fixed",
    targetTypes = Type.Struct,
    targetFormat = InMemoryFormat.JSON,
    configClass = FixedSchemaGeneratorConfig.class)
@Slf4j
public class JsonFixedSchemaStructGenerator extends FixedStructValueGenerator<JsonObject> {

  private final JsonSchema jsonSchema;

  public JsonFixedSchemaStructGenerator(FixedSchemaGeneratorConfig config) {
    super(config, JsonValueGeneratorFactory.getInstance());
    this.jsonSchema = new JsonSchema(config.getFieldConfig());
  }

  @Override
  public Object getPhysicalType() {
    //TODO: Build a better object later, align to JsonIntermediate Format
    return this.jsonSchema;
  }

  @Override
  public JsonObject get() {
    JsonObject record = new JsonObject();
    for (FieldConfigWithValueGenerator fieldConfigWithValueGenerator: this.logicalFieldGenerators) {
      ValueGenerator valueGenerator = fieldConfigWithValueGenerator.getValueGenerator();
      Object value = valueGenerator.get();
      record.add(fieldConfigWithValueGenerator.getFieldConfig().getName(), (JsonElement) value);
    }
    /**
    this.logicalFieldGenerators
        .forEach(fieldWithGenerator ->
            record.add(fieldWithGenerator.getFieldConfig().getName(),
                (JsonElement) fieldWithGenerator.getValueGenerator().get()));
     **/
    return record;
  }
}
