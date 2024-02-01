package io.confluent.connect.custom.transforms;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPathException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.*;

public class NestedValueToKeyTest {
    private final NestedValueToKey<SourceRecord> xform = new NestedValueToKey<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void testSimpleSchemaWithFieldExtraction() {

        Long expectedKey = 200L;
        xform.configure(Collections.singletonMap(NestedValueToKey.ConfigName.FIELD_CONFIG, "id"));
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("id", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("id", expectedKey);
        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                simpleStructSchema,
                simpleStruct
        );
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(expectedKey, transformedRecord.key());
        assertEquals("Value should not be modified", record.value(), transformedRecord.value());
        assertEquals(
                "Value schema should not be modified",
                record.valueSchema(),
                transformedRecord.valueSchema()
        );
    }

    @Test(expected = ConfigException.class)
    public void requiredConfigCannotBeNull() {
        xform.configure(Collections.emptyMap());
    }

    @Test(expected = InvalidPathException.class)
    public void providedInvalidJsonPath() {
        xform.configure(Collections.singletonMap(NestedValueToKey.ConfigName.FIELD_CONFIG, "$a$"));
    }

    @Test(expected = ConfigException.class)
    public void requiredConfigCannotBeEmpty() {
        xform.configure(Collections.singletonMap(NestedValueToKey.ConfigName.FIELD_CONFIG, ""));
    }

    @Test
    public void checkConfig(){
        assertTrue(xform.config().configKeys().containsKey(NestedValueToKey.ConfigName.FIELD_CONFIG));
    }

    @Test
    public void testWithUnavailableField() {
        xform.configure(Collections.singletonMap(NestedValueToKey.ConfigName.FIELD_CONFIG, "age"));
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("id", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("id", 100L);
        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                simpleStructSchema,
                simpleStruct
        );
        SourceRecord transformedRecord = xform.apply(record);
        assertNull(
                "Record should not be affected by missing optional field",
                transformedRecord.key()
        );
    }

    @Test
    public void testWithUnavailableFieldAndErrorField() {
        xform.configure(new HashMap<String, Object>(){{
            put(NestedValueToKey.ConfigName.FIELD_CONFIG, "age");
            put(NestedValueToKey.ConfigName.ERROR_MESSAGE_FIELD, "id");
        }});
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("id", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("id", 100L);
        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                simpleStructSchema,
                simpleStruct
        );
        SourceRecord transformedRecord = xform.apply(record);
        assertNull(
                "Record should not be affected by missing optional field",
                transformedRecord.key()
        );
    }

    @Test
    public void testWithUnavailableFieldWithKeyOnError() {
        String errorKey = "Undefined";
        xform.configure(new HashMap<String, Object>(){{
            put(NestedValueToKey.ConfigName.FIELD_CONFIG, "age");
            put(NestedValueToKey.ConfigName.KEY_ON_ERROR, errorKey);
        }});
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("id", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("id", 100L);
        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                simpleStructSchema,
                simpleStruct
        );
        SourceRecord transformedRecord = xform.apply(record);
        assertEquals(
                errorKey,
                transformedRecord.key()
        );
    }

    @Test
    public void FieldJsonPathSchemaless() {
        final String fieldName = "$.f1.field";
        final String expectedKey = "dummy";

        xform.configure(new HashMap<String, Object>(){{
            put(NestedValueToKey.ConfigName.FIELD_CONFIG, fieldName);
        }});

        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                null,
                new HashMap<String, Object>(){{
                    put("name", "test");
                    put( "f1",
                            new HashMap<String, Object>(){{
                                put(NestedValueToKey.ConfigName.FIELD_CONFIG, expectedKey);
                                put("f3", "dummy");
                            }}
                    );
                }}
        );
        SourceRecord transformedRecord = xform.apply(record);
        assertNull(
                "Record should not be affected by missing optional field",
                transformedRecord.key()
        );
    }

    @Test
    public void FieldJsonPathWithSchema() {
        final String fieldName = "$.f1.f3";
        final String expectedKey = "dummy";

        xform.configure(new HashMap<String, Object>(){{
            put(NestedValueToKey.ConfigName.FIELD_CONFIG, fieldName);
        }});

        final Schema nestedSchema = SchemaBuilder.struct()
                .field("field", Schema.STRING_SCHEMA)
                .field("f3", Schema.STRING_SCHEMA);

        final Schema arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA);

        final Schema schema =
                SchemaBuilder.struct()
                        .field("name", Schema.STRING_SCHEMA)
                        .field(
                                "f1",
                                nestedSchema
                        )
                        .field("books", arraySchema);


        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                schema,
                new Struct(schema){{
                    put("name", "field");
                    put( "f1",
                            new Struct(nestedSchema){{
                                put("field", "false");
                                put("f3", expectedKey);
                            }}
                    );
                    put("books", new ArrayList<String>(){{
                        add("book1");
                        add("book2");
                    }});
                }}
        );
        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals(
                expectedKey,
                transformedRecord.key()
        );
    }

    @Test
    public void complexNestedSchema() {
        final String fieldName = "$.properties.ACCTSUFF.integer.integer";
        final int expectedKey = 78;

        xform.configure(new HashMap<String, Object>(){{
            put(NestedValueToKey.ConfigName.FIELD_CONFIG, fieldName);
        }});

        final Schema integerSchema = SchemaBuilder.struct().field("integer", Schema.INT32_SCHEMA).optional().build();
        final Schema stringSchema = SchemaBuilder.struct().field("string", Schema.STRING_SCHEMA).optional().build();

        final Schema propertyValueSchema = SchemaBuilder.struct()
                .field("propertyType", Schema.STRING_SCHEMA)
                .field("integer", integerSchema)
                .field("string", stringSchema);

        final Schema propertyMapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, propertyValueSchema).build();


        final Schema schema =
                SchemaBuilder.struct()
                        .field("messageID", Schema.STRING_SCHEMA)
                        .field("messageType", Schema.STRING_SCHEMA)
                        .field("timestamp", Schema.INT64_SCHEMA)
                        .field("priority", Schema.INT32_SCHEMA)
                        .field("properties",propertyMapSchema).build();


        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                schema,
                new Struct(schema){{
                    put("messageID", "ID:414d512055544c4d5131484520202020a005b06502dcde96");
                    put("messageType", "text");
                            put("timestamp",1706545026030L);
                            put("priority",4);
                    put( "properties",
                            new HashMap<String, Struct>(){{
                                put("ACCTSUFF", new Struct(propertyValueSchema){{
                                    put("propertyType", "integer");
                                    put("integer", new Struct(integerSchema){{
                                       put("integer", 78);
                                    }});
                                    put("string", null);
                                }});
                            }});
                }}
        );
        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals(
                expectedKey,
                transformedRecord.key()
        );
    }
}
