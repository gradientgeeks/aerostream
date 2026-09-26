package schemaregistry

import (
	"bufio"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// checkCompatibility evaluates whether newSchema is compatible with oldSchema under the given level.
func checkCompatibility(level CompatibilityLevel, oldSchema, newSchema *Schema) (bool, error) {
	switch level {
	case NONE:
		return true, nil
	case BACKWARD, FORWARD, FULL:
		// proceed with validation
	default:
		return false, fmt.Errorf("%w: %s", ErrInvalidCompatLevel, level)
	}

	// Identical canonical schemas are unconditionally compatible
	if oldSchema.Schema == newSchema.Schema {
		return true, nil
	}

	// Schema types must match
	if strings.ToUpper(oldSchema.Type) != strings.ToUpper(newSchema.Type) {
		return false, fmt.Errorf("schema types mismatch: %s vs %s", oldSchema.Type, newSchema.Type)
	}

	sType := strings.ToUpper(newSchema.Type)
	switch sType {
	case TypeAvro:
		return checkAvroCompatibility(level, oldSchema.Schema, newSchema.Schema)
	case TypeJSON:
		return checkJSONCompatibility(level, oldSchema.Schema, newSchema.Schema)
	case TypeProtobuf:
		return checkProtobufCompatibility(level, oldSchema.Schema, newSchema.Schema)
	default:
		return false, fmt.Errorf("unsupported schema type: %s", sType)
	}
}

// --- Avro Compatibility ---

type avroField struct {
	name       string
	rawType    interface{}
	hasDefault bool
}

func checkAvroCompatibility(level CompatibilityLevel, oldStr, newStr string) (bool, error) {
	var oldObj, newObj interface{}
	if err := json.Unmarshal([]byte(oldStr), &oldObj); err != nil {
		return false, fmt.Errorf("invalid old avro schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newStr), &newObj); err != nil {
		return false, fmt.Errorf("invalid new avro schema: %w", err)
	}

	oldMap, oldIsMap := oldObj.(map[string]interface{})
	newMap, newIsMap := newObj.(map[string]interface{})

	// Both are record types
	if oldIsMap && newIsMap {
		oldFieldsRaw, oldHasFields := oldMap["fields"].([]interface{})
		newFieldsRaw, newHasFields := newMap["fields"].([]interface{})
		if oldHasFields && newHasFields {
			return checkAvroRecordFields(level, oldFieldsRaw, newFieldsRaw)
		}
	}

	// Non-record schemas (e.g. primitives, unions)
	if reflect.DeepEqual(oldObj, newObj) {
		return true, nil
	}
	return false, nil
}

func parseAvroFields(fieldsRaw []interface{}) map[string]avroField {
	result := make(map[string]avroField, len(fieldsRaw))
	for _, f := range fieldsRaw {
		fMap, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := fMap["name"].(string)
		if name == "" {
			continue
		}
		_, hasDefault := fMap["default"]
		result[name] = avroField{
			name:       name,
			rawType:    fMap["type"],
			hasDefault: hasDefault,
		}
	}
	return result
}

func checkAvroRecordFields(level CompatibilityLevel, oldFieldsRaw, newFieldsRaw []interface{}) (bool, error) {
	oldFields := parseAvroFields(oldFieldsRaw)
	newFields := parseAvroFields(newFieldsRaw)

	switch level {
	case BACKWARD:
		return isAvroBackwardCompatible(oldFields, newFields), nil
	case FORWARD:
		return isAvroForwardCompatible(oldFields, newFields), nil
	case FULL:
		if !isAvroBackwardCompatible(oldFields, newFields) {
			return false, nil
		}
		return isAvroForwardCompatible(oldFields, newFields), nil
	default:
		return false, fmt.Errorf("unknown compatibility level: %s", level)
	}
}

// BACKWARD: consumer using new schema can read data written with old schema.
// Rule 1: Every field in newFields NOT in oldFields (added field) MUST have a default.
// Rule 2: Every field common to both must have compatible types.
// Rule 3: Deleted fields are permitted.
func isAvroBackwardCompatible(oldFields, newFields map[string]avroField) bool {
	for name, newF := range newFields {
		oldF, exists := oldFields[name]
		if !exists {
			if !newF.hasDefault {
				return false
			}
		} else {
			if oldF.hasDefault && !newF.hasDefault {
				return false
			}
			if !areAvroTypesCompatible(oldF.rawType, newF.rawType) {
				return false
			}
		}
	}
	return true
}

// FORWARD: consumer using old schema can read data written with new schema.
// Rule 1: Every field in oldFields NOT in newFields (deleted field) MUST have had a default in old schema.
// Rule 2: Every field common to both must have compatible types.
// Rule 3: Added fields are permitted.
func isAvroForwardCompatible(oldFields, newFields map[string]avroField) bool {
	for name, oldF := range oldFields {
		newF, exists := newFields[name]
		if !exists {
			if !oldF.hasDefault {
				return false
			}
		} else {
			if !areAvroTypesCompatible(oldF.rawType, newF.rawType) {
				return false
			}
		}
	}
	return true
}

func areAvroTypesCompatible(oldType, newType interface{}) bool {
	if reflect.DeepEqual(oldType, newType) {
		return true
	}

	oldStr, oldIsStr := oldType.(string)
	newStr, newIsStr := newType.(string)
	if oldIsStr && newIsStr {
		if oldStr == newStr {
			return true
		}
		// Avro type promotions
		switch oldStr {
		case "int":
			return newStr == "long" || newStr == "float" || newStr == "double"
		case "long":
			return newStr == "float" || newStr == "double"
		case "float":
			return newStr == "double"
		case "string":
			return newStr == "bytes"
		case "bytes":
			return newStr == "string"
		}
	}

	// Compare serialized representations (for nested types or unions)
	oldJSON, _ := json.Marshal(oldType)
	newJSON, _ := json.Marshal(newType)
	return string(oldJSON) == string(newJSON)
}

// --- JSON Schema Compatibility ---

type jsonProperty struct {
	rawType    interface{}
	hasDefault bool
	required   bool
}

func checkJSONCompatibility(level CompatibilityLevel, oldStr, newStr string) (bool, error) {
	var oldObj, newObj interface{}
	if err := json.Unmarshal([]byte(oldStr), &oldObj); err != nil {
		return false, fmt.Errorf("invalid old JSON schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newStr), &newObj); err != nil {
		return false, fmt.Errorf("invalid new JSON schema: %w", err)
	}

	oldMap, oldIsMap := oldObj.(map[string]interface{})
	newMap, newIsMap := newObj.(map[string]interface{})

	if oldIsMap && newIsMap {
		oldPropsRaw, oldHasProps := oldMap["properties"].(map[string]interface{})
		newPropsRaw, newHasProps := newMap["properties"].(map[string]interface{})

		if oldHasProps && newHasProps {
			oldReq := parseRequiredList(oldMap["required"])
			newReq := parseRequiredList(newMap["required"])

			oldProps := parseJSONProperties(oldPropsRaw, oldReq)
			newProps := parseJSONProperties(newPropsRaw, newReq)

			switch level {
			case BACKWARD:
				return isJSONBackwardCompatible(oldProps, newProps), nil
			case FORWARD:
				return isJSONForwardCompatible(oldProps, newProps), nil
			case FULL:
				if !isJSONBackwardCompatible(oldProps, newProps) {
					return false, nil
				}
				return isJSONForwardCompatible(oldProps, newProps), nil
			}
		}
	}

	if reflect.DeepEqual(oldObj, newObj) {
		return true, nil
	}
	return false, nil
}

func parseRequiredList(raw interface{}) map[string]bool {
	reqMap := make(map[string]bool)
	if list, ok := raw.([]interface{}); ok {
		for _, item := range list {
			if s, ok := item.(string); ok {
				reqMap[s] = true
			}
		}
	}
	return reqMap
}

func parseJSONProperties(propsRaw map[string]interface{}, required map[string]bool) map[string]jsonProperty {
	res := make(map[string]jsonProperty, len(propsRaw))
	for name, def := range propsRaw {
		defMap, ok := def.(map[string]interface{})
		if !ok {
			res[name] = jsonProperty{required: required[name]}
			continue
		}
		_, hasDefault := defMap["default"]
		res[name] = jsonProperty{
			rawType:    defMap["type"],
			hasDefault: hasDefault,
			required:   required[name],
		}
	}
	return res
}

func isJSONBackwardCompatible(oldProps, newProps map[string]jsonProperty) bool {
	for name, newP := range newProps {
		oldP, exists := oldProps[name]
		if !exists {
			// Added property in new schema cannot be required unless it has a default
			if newP.required && !newP.hasDefault {
				return false
			}
		} else {
			if !reflect.DeepEqual(oldP.rawType, newP.rawType) {
				return false
			}
		}
	}
	return true
}

func isJSONForwardCompatible(oldProps, newProps map[string]jsonProperty) bool {
	for name, oldP := range oldProps {
		newP, exists := newProps[name]
		if !exists {
			// Deleted property from old schema cannot have been required unless it had a default
			if oldP.required && !oldP.hasDefault {
				return false
			}
		} else {
			if !reflect.DeepEqual(oldP.rawType, newP.rawType) {
				return false
			}
		}
	}
	return true
}

// --- Protobuf Compatibility ---

type protoFieldInfo struct {
	tag  string
	name string
	typ  string
}

var protoFieldRegex = regexp.MustCompile(`^\s*(?:optional|required|repeated)?\s*([a-zA-Z0-9_.]+)\s+([a-zA-Z0-9_]+)\s*=\s*([0-9]+)\s*;`)

func parseProtoFields(protoStr string) map[string]protoFieldInfo {
	fieldsByTag := make(map[string]protoFieldInfo)
	scanner := bufio.NewScanner(strings.NewReader(protoStr))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "//") || strings.HasPrefix(line, "/*") {
			continue
		}
		matches := protoFieldRegex.FindStringSubmatch(line)
		if len(matches) == 4 {
			typ := matches[1]
			name := matches[2]
			tag := matches[3]
			fieldsByTag[tag] = protoFieldInfo{
				tag:  tag,
				name: name,
				typ:  typ,
			}
		}
	}
	return fieldsByTag
}

func checkProtobufCompatibility(level CompatibilityLevel, oldStr, newStr string) (bool, error) {
	oldFields := parseProtoFields(oldStr)
	newFields := parseProtoFields(newStr)

	// Proto field tags must not change types
	for tag, oldF := range oldFields {
		if newF, exists := newFields[tag]; exists {
			if oldF.typ != newF.typ {
				return false, nil
			}
		}
	}

	switch level {
	case BACKWARD:
		// In proto3, adding fields with new tag numbers is backward compatible.
		return true, nil
	case FORWARD:
		// In proto3, deleting fields is forward compatible (old reader ignores unknown or keeps default).
		return true, nil
	case FULL:
		return true, nil
	default:
		return false, fmt.Errorf("unknown compatibility level: %s", level)
	}
}
