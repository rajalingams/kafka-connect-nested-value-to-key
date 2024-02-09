package io.confluent.connect.custom.transforms;

import io.confluent.connect.custom.utils.FieldJsonPathExtractor;
import io.confluent.connect.custom.utils.FieldListJsonPathExtractor;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class NestedValueToKeyAndHeader<R extends ConnectRecord<R>> extends BaseNestedValue<R> {
    private static final Logger log = LoggerFactory.getLogger(NestedValueToKeyAndHeader.class);


    public interface ConfigName {
        String FIELD_CONFIG = "field";
        String KEY_ON_ERROR = "keyOnError";
        String ERROR_MESSAGE_FIELD = "errorMessageField";
        String HEADER_FIELD_MAPPING = "headerFieldMapping";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Field name to be used as key, use json path")
            .define(ConfigName.KEY_ON_ERROR, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                    "Constant value to be used as key when there is an error")
            .define(ConfigName.ERROR_MESSAGE_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                    "Field name to be used when printing errors, use json path")
            .define(ConfigName.HEADER_FIELD_MAPPING, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW,
                    "Map of header field name to json path in the message value. eg: field1:jsonpath1,field2:jsonpath2..");

    private static final String PURPOSE = "construct the record key and header from value";

    private String field;
    private String errorField;
    private String keyOnError;
    private List<String> headerFieldList;
    private Map<String, String> headerFieldMap;
    private Function<Object, Object> fieldExtractor = null;
    private Function<Object, Object> errorFieldExtractor = null;
    private FieldListJsonPathExtractor headerFieldExtractor = null;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        field = config.getString(ConfigName.FIELD_CONFIG);
        keyOnError = config.getString(ConfigName.KEY_ON_ERROR);
        errorField = config.getString(ConfigName.ERROR_MESSAGE_FIELD);
        headerFieldList = config.getList(ConfigName.HEADER_FIELD_MAPPING);
        if (this.field == null || this.field.isBlank()) {
            throw new ConfigException(
                    "`" + ConfigName.FIELD_CONFIG + "` is required for `" + getClass().getName() + "`"
            );
        }
        fieldExtractor = new FieldJsonPathExtractor(field, ConfigName.FIELD_CONFIG);
        if (this.errorField != null && !this.errorField.isBlank()) {
            errorFieldExtractor = new FieldJsonPathExtractor(errorField, ConfigName.ERROR_MESSAGE_FIELD);
        }
        if (this.headerFieldList != null && !this.headerFieldList.isEmpty()) {
            headerFieldMap = parseMappings(this.headerFieldList, ConfigName.HEADER_FIELD_MAPPING);
            headerFieldExtractor = new FieldListJsonPathExtractor(headerFieldMap, ConfigName.HEADER_FIELD_MAPPING);
        }
    }

    @Override
    protected R applySchemaless(R record) {
        final Map<String, Object> messageValue = requireMap(record.value(), PURPOSE);
        return record.newRecord(record.topic(), null, null, getKey(messageValue),
                record.valueSchema(), record.value(), record.timestamp(), getHeaders(record, messageValue));
    }

    @Override
    protected R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        Object messageValue = extractObject(record);
        return record.newRecord(record.topic(), null, null, getKey(messageValue),
                value.schema(), value, record.timestamp(), getHeaders(record, messageValue));
    }

    private Headers getHeaders(R record, Object messageValue) {
        Headers headers = record.headers().duplicate();
        if (headerFieldExtractor != null) {
            for (Map.Entry<String, String> fieldItem : headerFieldMap.entrySet()) {
                headers.add(fieldItem.getKey(), headerFieldExtractor.extractValue(fieldItem.getKey(), messageValue), null);
            }
        }
        return headers;
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

