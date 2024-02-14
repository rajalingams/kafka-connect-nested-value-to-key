package io.confluent.connect.custom.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.custom.utils.MessageConverter.convertObject;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class BaseNestedValue<R extends ConnectRecord<R>> implements Transformation<R> {
    public Object extractObject(R record, String purpose) {
        if (record.valueSchema() == null)
            return requireMap(record.value(), purpose);
        else
            return convertObject(
                    requireStruct(record.value(), purpose),
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
