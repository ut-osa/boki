package statestore

const (
	VALUE_Number = iota
	VALUE_String
	VALUE_Bool
	VALUE_Null
	VALUE_EmptyObject
	VALUE_EmptyArray
	VALUE_Object
	VALUE_Array
)

type Value struct {
	ValueType   int                    `json:"t"`
	NumberValue float64                `json:"n,omitempty"`
	StringValue string                 `json:"s,omitempty"`
	BoolValue   bool                   `json:"b,omitempty"`
	Object      map[string]interface{} `json:"-"`
	Array       []interface{}          `json:"-"`
}

func (v *Value) IsNull() bool {
	return v.ValueType == VALUE_Null
}

func (v *Value) AsNumber() float64 {
	if v.ValueType != VALUE_Number {
		panic("Not a number type")
	}
	return v.NumberValue
}

func (v *Value) AsString() string {
	if v.ValueType != VALUE_String {
		panic("Not a string type")
	}
	return v.StringValue
}

func (v *Value) AsBool() bool {
	if v.ValueType != VALUE_Bool {
		panic("Not a boolean type")
	}
	return v.BoolValue
}

func (v *Value) AsObject() map[string]interface{} {
	if v.ValueType == VALUE_EmptyObject {
		return make(map[string]interface{})
	}
	if v.ValueType != VALUE_Object {
		panic("Not an object type")
	}
	return v.Object
}

func (v *Value) AsArray() []interface{} {
	if v.ValueType == VALUE_EmptyArray {
		return make([]interface{}, 0)
	}
	if v.ValueType != VALUE_Array {
		panic("Not an array type")
	}
	return v.Array
}

func (v *Value) asInterface() interface{} {
	switch v.ValueType {
	case VALUE_Number:
		return v.NumberValue
	case VALUE_String:
		return v.StringValue
	case VALUE_Bool:
		return v.BoolValue
	case VALUE_Null:
		return nil
	case VALUE_EmptyObject:
		return make(map[string]interface{})
	case VALUE_EmptyArray:
		return make([]interface{}, 0)
	case VALUE_Object:
		return v.Object
	case VALUE_Array:
		return v.Array
	default:
		panic("Unknown value type")
	}
}

func NumberValue(value float64) Value {
	return Value{ValueType: VALUE_Number, NumberValue: value}
}

func StringValue(value string) Value {
	return Value{ValueType: VALUE_String, StringValue: value}
}

func BoolValue(value bool) Value {
	return Value{ValueType: VALUE_Bool, BoolValue: value}
}

func NullValue() Value {
	return Value{ValueType: VALUE_Null}
}

func EmptyObjectValue() Value {
	return Value{ValueType: VALUE_EmptyObject}
}

func EmptyArrayValue() Value {
	return Value{ValueType: VALUE_EmptyArray}
}

func valueFromInterface(contents interface{}) Value {
	switch v := contents.(type) {
	case bool:
		return BoolValue(v)
	case float64:
		return NumberValue(v)
	case string:
		return StringValue(v)
	case map[string]interface{}:
		return Value{ValueType: VALUE_Object, Object: v}
	case []interface{}:
		return Value{ValueType: VALUE_Object, Array: v}
	case nil:
		return NullValue()
	default:
		panic("Failed to determine type")
	}
}
