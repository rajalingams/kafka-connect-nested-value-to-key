package io.confluent.connect.custom.transforms;

import io.confluent.connect.custom.utils.FieldListJsonPathExtractor;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.List;
import java.util.Map;

public class NestedValueToHeader<R extends ConnectRecord<R>> extends BaseNestedValue<R> {

    public interface ConfigName {
        String HEADER_FIELD_MAPPING = "headerFieldMapping";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.HEADER_FIELD_MAPPING, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW,
                    "Map of header field name to json path in the message value. eg: field1:jsonpath1,field2:jsonpath2..");

    private static final String PURPOSE = "construct the record header from value";
    private List<String> headerFieldList;
    private Map<String, String> headerFieldMap;
    private FieldListJsonPathExtractor headerFieldExtractor = null;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerFieldList = config.getList(NestedValueToHeader.ConfigName.HEADER_FIELD_MAPPING);
        if (this.headerFieldList == null || this.headerFieldList.isEmpty()) {
            throw new ConfigException(
                    "`" + ConfigName.HEADER_FIELD_MAPPING + "` is required for `" + getClass().getName() + "`"
            );
        }
        headerFieldMap = parseMappings(this.headerFieldList, ConfigName.HEADER_FIELD_MAPPING);
        headerFieldExtractor = new FieldListJsonPathExtractor(headerFieldMap, ConfigName.HEADER_FIELD_MAPPING);
    }

    @Override
    public R apply(R record) {
        Object messageValue = extractObject(record, PURPOSE);
        return record.newRecord(record.topic(), null, null, record.key(),
                record.valueSchema(), record.value(), record.timestamp(), getHeaders(record, messageValue));
    }

    private Headers getHeaders(R record, Object messageValue) {
        Headers headers = record.headers().duplicate();
        for (Map.Entry<String, String> fieldItem : headerFieldMap.entrySet()) {
            headers.add(fieldItem.getKey(), headerFieldExtractor.extractValue(fieldItem.getKey(), messageValue), null);
        }
        return headers;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}

