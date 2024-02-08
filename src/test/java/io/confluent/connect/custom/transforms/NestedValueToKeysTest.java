package io.confluent.connect.custom.transforms;

import com.jayway.jsonpath.InvalidPathException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NestedValueToKeysTest {
    private final NestedValueToKeys<SourceRecord> xform = new NestedValueToKeys<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test(expected = ConfigException.class)
    public void requiredConfigCannotBeNull() {
        xform.configure(Collections.emptyMap());
    }


    @Test
    public void checkConfig() {
        assertTrue(xform.config().configKeys().containsKey(NestedValueToKeys.ConfigName.KEY_FIELD_MAPPING));
    }

    @Test
    public void FieldJsonPathSchemalessHeader() {
        final Map<String, Object> expectedKeys = new LinkedHashMap<>();
        expectedKeys.put("fullname", "test");
        xform.configure(new HashMap<String, Object>() {{
            put(NestedValueToKeys.ConfigName.KEY_FIELD_MAPPING, "fullname:name");
        }});

        final SourceRecord record = new SourceRecord(
                null,
                null,
                "test",
                null,
                "key",
                null,
                new HashMap<String, Object>() {{
                    put("name", "test");
                    put("f1",
                            new HashMap<String, Object>() {{
                                put("f2", "test");
                                put("f3", "dummy");
                            }}
                    );
                }}
        );
        SourceRecord transformedRecord = xform.apply(record);
        assertEquals(expectedKeys, transformedRecord.key());
    }

    @Test
    public void FieldJsonPathWithSchemaAndHeader() {
        final Map<String, Object> expectedKeys = new LinkedHashMap<>();
        expectedKeys.put("fullname", "somename");
        expectedKeys.put("fieldVal", "dummy");

        xform.configure(new HashMap<String, Object>() {{
            put(NestedValueToKeys.ConfigName.KEY_FIELD_MAPPING, "fullname:$.name,fieldVal:$.f1.field");
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
                new Struct(schema) {{
                    put("name", "somename");
                    put("f1",
                            new Struct(nestedSchema) {{
                                put("field", "dummy");
                                put("f3", "rome");
                            }}
                    );
                    put("books", new ArrayList<String>() {{
                        add("book1");
                        add("book2");
                    }});
                }}
        );
        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals(expectedKeys, transformedRecord.key());
    }

    @Test(expected = ConfigException.class)
    public void headerFieldMappingShouldBeFormattedCorrectly() {
        xform.configure(new HashMap<String, Object>() {{
            put(NestedValueToKeys.ConfigName.KEY_FIELD_MAPPING, "fullname$.name");
        }});
    }

    @Test(expected = InvalidPathException.class)
    public void providedInvalidJsonPathInHeaderMap() {
        xform.configure(new HashMap<String, Object>() {{
            put(NestedValueToKeys.ConfigName.KEY_FIELD_MAPPING, "fullname:$a$");
        }});
    }

    @Test
    public void FieldJsonPathWithSchemaAndHeaderPathNotFound() {
        final Map<String, Object> expectedKeys = new LinkedHashMap<>();
        expectedKeys.put("fullname", "somename");
        expectedKeys.put("fieldVal", null);

        xform.configure(new HashMap<String, Object>() {{
            put(NestedValueToKeys.ConfigName.KEY_FIELD_MAPPING, "fullname:$.name,fieldVal:$.f1.x.field");
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
                new Struct(schema) {{
                    put("name", "somename");
                    put("f1",
                            new Struct(nestedSchema) {{
                                put("field", "dummy");
                                put("f3", "rome");
                            }}
                    );
                    put("books", new ArrayList<String>() {{
                        add("book1");
                        add("book2");
                    }});
                }}
        );
        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals(expectedKeys, transformedRecord.key());
    }
}
