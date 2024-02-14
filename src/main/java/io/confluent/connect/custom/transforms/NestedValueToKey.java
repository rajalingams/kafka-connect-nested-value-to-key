package io.confluent.connect.custom.transforms;

import io.confluent.connect.custom.utils.FieldJsonPathExtractor;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

public class NestedValueToKey<R extends ConnectRecord<R>> extends BaseNestedValue<R> {
    private static final Logger log = LoggerFactory.getLogger(NestedValueToKey.class);


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
    private Function<Object, Object> fieldExtractor = null;
    private Function<Object, Object> errorFieldExtractor = null;

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
        fieldExtractor = new FieldJsonPathExtractor(field, ConfigName.FIELD_CONFIG);
        if (this.errorField != null && !this.errorField.isBlank()) {
            errorFieldExtractor = new FieldJsonPathExtractor(errorField, ConfigName.ERROR_MESSAGE_FIELD);
        }
    }

    @Override
    public R apply(R record) {
        Object messageValue = extractObject(record, PURPOSE);
        return record.newRecord(record.topic(), null, null, getKey(messageValue), record.valueSchema(), record.value(), record.timestamp(), record.headers());
    }

    private Object getKey(Object messageValue) {
        Object keyObject;
        keyObject = fieldExtractor.apply(messageValue);
        if (keyObject == null || keyObject.toString().isBlank()) {
            printWarn(messageValue, "The key value for the field {} is null or blank", field);
            keyObject = keyOnError;
        }
        return keyObject;
    }

    private void printWarn(Object data, String message, Object... args) {
        if (errorFieldExtractor != null) {
            Object errorFieldValue = errorFieldExtractor.apply(data);
            if (errorFieldValue == null)
                log.debug("Specified json path {} for error field is not available in {}", errorField, data);
            log.warn(String.format("%s. Error field: %s", message, errorFieldValue), args);
        } else {
            log.warn(message, args);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}

