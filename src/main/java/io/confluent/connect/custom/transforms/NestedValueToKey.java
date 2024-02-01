package io.confluent.connect.custom.transforms;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.confluent.connect.custom.utils.TypeConverter.convertObject;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class NestedValueToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(NestedValueToKey.class);

    private static final String FIELD_EXTRACTION_PURPOSE = "field extraction";

    public interface ConfigName {
        String FIELD_CONFIG = "field";
        String KEY_ON_ERROR = "keyOnError";
        String ERROR_MESSAGE_FIELD = "errorMessageField";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Field name to be used as key, use json path")
            .define(ConfigName.KEY_ON_ERROR, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                    "Constant value to be used as key when there is an error")
            .define(ConfigName.ERROR_MESSAGE_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                    "Field name to be used when printing errors, use json path");

    private static final String PURPOSE = "construct the record key from value";

    private String field;
    private String errorField;
    private String keyOnError;
    private Function<R, Object> fieldExtractor = null;
    private Function<R, Object> errorFieldExtractor = null;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        field = config.getString(ConfigName.FIELD_CONFIG);
        keyOnError = config.getString(ConfigName.KEY_ON_ERROR);
        errorField = config.getString(ConfigName.ERROR_MESSAGE_FIELD);
        if (this.field == null || this.field.isBlank()) {
            throw new ConfigException(
                    "`" + ConfigName.FIELD_CONFIG + "` is required for `" + getClass().getName() + "`"
            );
        }
        fieldExtractor = new FieldJsonPathExtractor(field);
        if (this.errorField != null && !this.errorField.isBlank()) {
            errorFieldExtractor = new FieldJsonPathExtractor(errorField, true);
        }
    }


    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        return record.newRecord(record.topic(), null, null, value.getOrDefault(field, keyOnError), record.valueSchema(), record.value(), record.timestamp());
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        Object keyObject = null;
        if (record.value() != null) {
            keyObject = fieldExtractor.apply(record);
        }
        if (keyObject == null || keyObject.toString().isBlank()) {
            printWarn(record, "The key value for the field {} is null or blank", field);
            keyObject = keyOnError;
        }
        return record.newRecord(record.topic(), null, null, keyObject, value.schema(), value, record.timestamp());
    }


    private void printWarn(R record, String message, Object... args) {
        if (errorFieldExtractor != null) {
            Object errorFieldValue = errorFieldExtractor.apply(record);
            log.warn(String.format("%s. Error field: %s", message, errorFieldValue), args);
        } else {
            log.warn(message, args);
        }
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    protected class FieldJsonPathExtractor implements Function<R, Object> {
        private final String fieldName;
        private final JsonPath field;
        private Boolean isErrorField = false;

        public FieldJsonPathExtractor(String fieldName) {
            try {
                this.fieldName = Objects.requireNonNull(fieldName, "Field name cannot be null");
                this.field = JsonPath.compile(this.fieldName);
            } catch (InvalidPathException e) {
                throw new InvalidPathException("Json Path `" + fieldName + "`specified in `"
                        + ConfigName.FIELD_CONFIG + "`config is incorrectly formatted. "
                        + "Please refer to com.jayway.jsonpath java doc for correct use of jsonpath.");
            }
        }

        public FieldJsonPathExtractor(String fieldName, Boolean errorField) {
            this(fieldName);
            this.isErrorField = errorField;
        }

            @Override
        public Object apply(R record) {
            Object data = convertObject(
                    requireStruct(record.value(), FIELD_EXTRACTION_PURPOSE),
                    record.valueSchema()
            );
            Object fieldValue;
            try {
                // not use DEFAULT_PATH_LEAF_TO_NULL since it does not
                // deal with $.a.b.c where b is already missing
                fieldValue = this.field.read(data);
            } catch (PathNotFoundException e) {
                if (!isErrorField)
                    printWarn(record, "Specified json path {} for field is not available", fieldName);
                fieldValue = null;
            }
            return fieldValue;
        }
    }


}

