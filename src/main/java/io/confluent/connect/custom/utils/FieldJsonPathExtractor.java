package io.confluent.connect.custom.utils;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import java.util.Objects;
import java.util.function.Function;

public class FieldJsonPathExtractor implements Function<Object, Object> {
    private final JsonPath field;

    public FieldJsonPathExtractor(String fieldName, String config) {
        try {
            String fieldNameParam = Objects.requireNonNull(fieldName, "Field name cannot be null");
            this.field = JsonPath.compile(fieldNameParam);
        } catch (InvalidPathException e) {
            throw new InvalidPathException("Json Path `" + fieldName + "`specified in `"
                    + config + "`config is incorrectly formatted. "
                    + "Please refer to com.jayway.jsonpath java doc for correct use of jsonpath.");
        }
    }

    @Override
    public Object apply(Object data) {
        try {
            return this.field.read(data);
        } catch (PathNotFoundException e) {
            return null;
        }
    }
}
