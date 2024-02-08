package io.confluent.connect.custom.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.custom.utils.TypeConverter.convertObject;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class BaseNestedValue<R extends ConnectRecord<R>>  implements Transformation<R> {
    private static final String FIELD_EXTRACTION_PURPOSE = "field extraction";


    protected abstract R applySchemaless(R record);
    protected abstract R applyWithSchema(R record);

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    public Object extractObject(R record) {
        return convertObject(
                requireStruct(record.value(), FIELD_EXTRACTION_PURPOSE),
                record.valueSchema()
        );
    }

    static Map<String, String> parseMappings(List<String> mappings, String config) {
        final Map<String, String> m = new LinkedHashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(config, mappings, "Invalid mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
        }
        return m;
    }

    @Override
    public void close() {
    }
}
