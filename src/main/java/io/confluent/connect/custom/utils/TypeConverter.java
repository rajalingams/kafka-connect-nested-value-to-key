/*
 * Copyright [2018 - 2019] Confluent Inc.
 */

package io.confluent.connect.custom.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TypeConverter {
  private static Map<String, Object> convertStruct(Struct kafkaConnectStruct,
                                                  Schema kafkaConnectSchema) {
    Map<String, Object> record = new HashMap<>();

    for (Field kafkaConnectField : kafkaConnectSchema.fields()) {
      Object value = convertObject(
              kafkaConnectStruct.get(kafkaConnectField.name()),
              kafkaConnectField.schema()
      );
      if (value != null) {
        record.put(kafkaConnectField.name(), value);
      }
    }
    return record;
  }

  @SuppressWarnings("unchecked")
  public static Object convertObject(Object kafkaConnectObject, Schema kafkaConnectSchema) {
    if (kafkaConnectObject == null) {
      if (kafkaConnectSchema.isOptional()) {
        // short circuit converting the object
        return null;
      } else {
        throw new DataException(
          kafkaConnectSchema.name() + " is not optional, but converting object had null value");
      }
    }
    if (kafkaConnectSchema.type().isPrimitive()) {
      return kafkaConnectObject;
    }

    Schema.Type kafkaConnectSchemaType = kafkaConnectSchema.type();
    switch (kafkaConnectSchemaType) {
      case ARRAY:
        return convertArray((List<Object>) kafkaConnectObject, kafkaConnectSchema);
      case MAP:
        return convertMap((Map<Object, Object>)kafkaConnectObject, kafkaConnectSchema);
      case STRUCT:
        return convertStruct((Struct) kafkaConnectObject, kafkaConnectSchema);
      default:
        throw new DataException("Unrecognized schema type: " + kafkaConnectSchemaType);
    }
  }

  private static List<Object> convertArray(List<Object> kafkaConnectList,
                                           Schema kafkaConnectSchema) {
    Schema kafkaConnectValueSchema = kafkaConnectSchema.valueSchema();
    List<Object> list = new ArrayList<>();
    for (Object kafkaConnectElement : kafkaConnectList) {
      Object element = convertObject(kafkaConnectElement, kafkaConnectValueSchema);
      list.add(element);
    }
    return list;
  }

  private static Object convertMap(Map<Object, Object> kafkaConnectMap,
                                                      Schema kafkaConnectSchema) {
    Schema kafkaConnectKeySchema = kafkaConnectSchema.keySchema();
    Schema kafkaConnectValueSchema = kafkaConnectSchema.valueSchema();

    List<Map<String, Object>> entryList = new ArrayList<>();
    Map<Object, Object> map = new HashMap<>();

    boolean isMap = kafkaConnectKeySchema.type() == Schema.Type.STRING;

    for (Map.Entry<Object, Object> kafkaConnectMapEntry : kafkaConnectMap.entrySet()) {
      Map<String, Object> entry = new HashMap<>();
      Object key = convertObject(
              kafkaConnectMapEntry.getKey(),
              kafkaConnectKeySchema
      );
      Object value = convertObject(
              kafkaConnectMapEntry.getValue(),
              kafkaConnectValueSchema
      );

      if (isMap) {
        map.put(key, value);
      } else {
        entry.put("key", key);
        entry.put("value", value);
        entryList.add(entry);
      }
    }

    return isMap ? map : entryList;
  }
}