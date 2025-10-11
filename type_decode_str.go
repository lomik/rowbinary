package rowbinary

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func quote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return s
	}
	s = s[1 : len(s)-1]
	s = strings.ReplaceAll(s, `\'`, `'`)
	s = strings.ReplaceAll(s, `\\`, `\`)
	return s
}

func decodeStringTypeSplitRoot(t string, sep byte) []string {
	s := t
	var ret []string

	i := 0
	level := 0
	for i < len(s) {
		if level == 0 && s[i] == sep {
			ret = append(ret, strings.TrimSpace(s[:i]))
			s = s[i+1:]
			i = 0
			continue
		}
		if s[i] == '(' {
			level += 1
		}
		if s[i] == ')' {
			level -= 1
		}
		i++
	}

	ret = append(ret, strings.TrimSpace(s))
	return ret
}

func decodeStringTypeParseFunc(t string) (string, []string, error) {
	s := strings.TrimSpace(t)
	if !strings.Contains(s, "(") {
		return s, nil, nil
	}
	if len(s) == 0 {
		return "", nil, fmt.Errorf("can't parse func %#v", s)
	}
	if s[len(s)-1] != ')' {
		return "", nil, fmt.Errorf("can't parse func %#v", s)
	}

	var funcName string
	var args []string

	funcName = s[:strings.Index(t, "(")]
	argsStr := s[strings.Index(t, "(")+1 : len(s)-1]

	args = decodeStringTypeSplitRoot(argsStr, ',')

	return funcName, args, nil
}

// DecodeStringType decodes a string type from the given reader.
func DecodeStringType(t string) (Any, error) {
	// simple types
	switch strings.TrimSpace(t) {
	case "Nothing":
		return Nothing, nil
	case "Bool":
		return Bool, nil
	case "UInt8":
		return UInt8, nil
	case "UInt16":
		return UInt16, nil
	case "UInt32":
		return UInt32, nil
	case "UInt64":
		return UInt64, nil
	case "UInt128":
		return nil, errors.New("not implemented")
	case "UInt256":
		return nil, errors.New("not implemented")
	case "Int8":
		return Int8, nil
	case "Int16":
		return Int16, nil
	case "Int32":
		return Int32, nil
	case "Int64":
		return Int64, nil
	case "Int128":
		return nil, errors.New("not implemented")
	case "Int256":
		return nil, errors.New("not implemented")
	case "Float32":
		return Float32, nil
	case "Float64":
		return Float64, nil
	case "Date":
		return Date, nil
	case "Date32":
		return Date32, nil
	case "DateTime":
		return DateTime, nil
	case "DateTime64":
		return nil, errors.New("not implemented")
	case "String":
		return String, nil
	case "FixedString": // <var_uint_size>
		return nil, errors.New("not implemented")
	case "UUID":
		return UUID, nil
	case "IPv4":
		return IPv4, nil
	case "IPv6":
		return IPv6, nil
	case "Dynamic":
		return DynamicAny(32), nil
	}

	funcName, funcArgs, err := decodeStringTypeParseFunc(t)
	if err != nil {
		return nil, err
	}

	switch funcName {
	case "Array":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("Array must have exactly one argument: %#v", t)
		}
		elemType, err := DecodeStringType(funcArgs[0])
		if err != nil {
			return nil, err
		}
		return ArrayAny(elemType), nil

	case "Map":
		if len(funcArgs) != 2 {
			return nil, fmt.Errorf("Map must have exactly two arguments: %#v", t)
		}
		keyType, err := DecodeStringType(funcArgs[0])
		if err != nil {
			return nil, err
		}
		valueType, err := DecodeStringType(funcArgs[1])
		if err != nil {
			return nil, err
		}
		return MapAny(keyType, valueType), nil

	case "Nullable":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("Nullable must have exactly one argument: %#v", t)
		}
		elemType, err := DecodeStringType(funcArgs[0])
		if err != nil {
			return nil, err
		}
		return NullableAny(elemType), nil

	case "LowCardinality":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("LowCardinality must have exactly one argument: %#v", t)
		}
		elemType, err := DecodeStringType(funcArgs[0])
		if err != nil {
			return nil, err
		}
		return LowCardinalityAny(elemType), nil

	case "DateTime":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("DateTime must have exactly one argument: %#v", t)
		}
		return DateTimeTZ(unquote(funcArgs[0])), nil

	case "DateTime64":
		if len(funcArgs) < 1 {
			return nil, fmt.Errorf("DateTime64 must have at least one argument: %#v", t)
		}
		if len(funcArgs) > 2 {
			return nil, fmt.Errorf("DateTime64 must have at most two arguments: %#v", t)
		}
		precision, err := strconv.Atoi(funcArgs[0])
		if err != nil {
			return nil, fmt.Errorf("can't parse DateTime64 precision: %w", err)
		}
		if len(funcArgs) == 1 {
			return DateTime64(uint8(precision)), nil
		}
		return DateTime64TZ(uint8(precision), unquote(funcArgs[1])), nil

	case "Decimal":
		if len(funcArgs) != 2 {
			return nil, fmt.Errorf("Decimal must have exactly two arguments: %#v", t)
		}
		precision, err := strconv.Atoi(funcArgs[0])
		if err != nil {
			return nil, fmt.Errorf("can't parse Decimal precision: %w", err)
		}
		scale, err := strconv.Atoi(funcArgs[1])
		if err != nil {
			return nil, fmt.Errorf("can't parse Decimal scale: %w", err)
		}
		return Decimal(uint8(precision), uint8(scale)), nil

	case "FixedString":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("FixedString must have exactly one argument: %#v", t)
		}
		size, err := strconv.Atoi(funcArgs[0])
		if err != nil {
			return nil, fmt.Errorf("can't parse FixedString size: %w", err)
		}
		return FixedString(size), nil

	case "Tuple":
		if len(funcArgs) == 0 {
			return nil, fmt.Errorf("Tuple must have at least one argument: %#v", t)
		}

		firstArgArr := decodeStringTypeSplitRoot(funcArgs[0], ' ')

		if len(firstArgArr) == 1 {
			var elemTypes []Any
			for _, arg := range funcArgs {
				elemType, err := DecodeStringType(arg)
				if err != nil {
					return nil, err
				}
				elemTypes = append(elemTypes, elemType)
			}
			return TupleAny(elemTypes...), nil
		}

		// named tuple
		var columns []Column
		for _, arg := range funcArgs {
			argArr := decodeStringTypeSplitRoot(arg, ' ')
			if len(argArr) != 2 {
				return nil, fmt.Errorf("can't parse named tuple element: %#v", arg)
			}
			elemType, err := DecodeStringType(argArr[1])
			if err != nil {
				return nil, err
			}
			columns = append(columns, Column{name: argArr[0], tp: elemType})
		}
		return TupleNamedAny(columns...), nil

	case "Enum8":
		// Enum8('windows' = -10, 'android' = 1, 'ios' = 2)
		mp := make(map[string]int8)
		for _, arg := range funcArgs {
			argArr := decodeStringTypeSplitRoot(arg, ' ')
			if len(argArr) != 3 || argArr[1] != "=" {
				return nil, fmt.Errorf("can't parse enum element: %#v", arg)
			}
			name := unquote(argArr[0])
			value, err := strconv.Atoi(argArr[2])
			if err != nil {
				return nil, fmt.Errorf("can't parse enum value: %w", err)
			}
			mp[name] = int8(value)
		}
		return Enum8(mp), nil
	case "Enum16":
		// Enum16('windows' = -10, 'android' = 1, 'ios' = 2)
		mp := make(map[string]int16)
		for _, arg := range funcArgs {
			argArr := decodeStringTypeSplitRoot(arg, ' ')
			if len(argArr) != 3 || argArr[1] != "=" {
				return nil, fmt.Errorf("can't parse enum element: %#v", arg)
			}
			name := unquote(argArr[0])
			value, err := strconv.Atoi(argArr[2])
			if err != nil {
				return nil, fmt.Errorf("can't parse enum value: %w", err)
			}
			mp[name] = int16(value)
		}
		return Enum16(mp), nil

	case "Variant":
		if len(funcArgs) == 0 {
			return nil, fmt.Errorf("Variant must have at least one argument: %#v", t)
		}

		var elemTypes []Any
		for _, arg := range funcArgs {
			elemType, err := DecodeStringType(arg)
			if err != nil {
				return nil, err
			}
			elemTypes = append(elemTypes, elemType)
		}
		return VariantAny(elemTypes...), nil

	case "Dynamic":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("Dynamic must have exactly one argument: %#v", t)
		}

		argArr := decodeStringTypeSplitRoot(funcArgs[0], '=')
		if len(argArr) != 2 {
			return nil, fmt.Errorf("can't parse type: %#v", t)
		}
		if argArr[0] != "max_types" {
			return nil, fmt.Errorf("can't parse type: %#v", t)
		}

		maxTypes, err := strconv.Atoi(argArr[1])
		if err != nil {
			return nil, fmt.Errorf("can't parse max_types: %w", err)
		}
		return DynamicAny(uint8(maxTypes)), nil
	}

	if !strings.Contains(t, "(") {
		return Custom(t, Nothing), nil
	}

	return nil, fmt.Errorf("can' parse type: %#v", t)
}
