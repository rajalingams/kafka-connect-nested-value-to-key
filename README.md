Custom Kafka Connect SMT to set the value from body as Key using Json path
The field/errorMessageField can be specified using [JsonPath](https://github.com/json-path/JsonPath)
The raw value of the field will be set as the message key.

This SMT supports setting the Key from the message body.
Properties:

| Name                | Description                                               | Type   | Default | Required | Importance |
|---------------------|-----------------------------------------------------------|--------|---------|----------|------------|
| `field`             | Field name to be used as key, use json path               | String | null    | true     | High       |
| `keyOnError`        | Constant value to be used as key when there is an error   | String | null    | false    | Low        |
| `errorMessageField` | Field name to be used when printing errors, use json path | String | null    | false    | Low        |

Example on how to add to your connector:

```
transforms=nestedValueToKey
transforms.nestedValueToKey.type=io.confluent.connect.custom.transforms.NestedValueToKey
transforms.nestedValueToKey.field="$.name"
transforms.nestedValueToKey.keyOnError="John Doe"
transforms.nestedValueToKey.errorMessageField="$.id"
```