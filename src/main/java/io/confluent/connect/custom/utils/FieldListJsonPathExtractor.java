package io.confluent.connect.custom.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class FieldListJsonPathExtractor {
    private final Map<String, Function<Object, Object>> fields = new LinkedHashMap<>();

    public FieldListJsonPathExtractor(Map<String, String> fieldMap, String config) {
        for (Map.Entry<String, String> fieldItem : fieldMap.entrySet()) {
            fields.put(fieldItem.getKey(), new FieldJsonPathExtractor(fieldItem.getValue(), config));
        }
    }

    public Map<String, Object> extractValues(Object data) {
        Map<String, Object> fieldValues = new LinkedHashMap<>();
        for (Map.Entry<String, Function<Object, Object>> fieldItem : fields.entrySet()) {
            fieldValues.put(fieldItem.getKey(), fieldItem.getValue().apply(data));
        }
        return fieldValues;
    }

    public Object extractValue(String key, Object data) {
        return fields.get(key).apply(data);
    }
}
