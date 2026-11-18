package rowbinary

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

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
		if s[i] == '\'' {
			i = skipQuoted(s, i)
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

// checkTypeName rejects empty names and unbalanced brackets outside quoted
// strings, so malformed names are not silently accepted as Custom types.
func checkTypeName(t string) error {
	s := strings.TrimSpace(t)
	if s == "" {
		return errors.New("empty type name")
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			for i++; i < len(s) && s[i] != '\''; i++ {
				if s[i] == '\\' {
					i++
				}
			}
			if i >= len(s) {
				return fmt.Errorf("unterminated quote in type name %q", s)
			}
		case '(':
			depth++
			if depth > maxTypeDepth {
				return fmt.Errorf("type name nested deeper than %d levels", maxTypeDepth)
			}
		case ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced brackets in type name %q", s)
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("unbalanced brackets in type name %q", s)
	}
	return nil
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

	funcName = s[:strings.Index(s, "(")]
	argsStr := s[strings.Index(s, "(")+1 : len(s)-1]

	if strings.TrimSpace(argsStr) != "" {
		args = decodeStringTypeSplitRoot(argsStr, ',')
	}

	return funcName, args, nil
}

// DecodeStringType decodes a string type from the given reader.
// Types nested deeper than maxTypeDepth are rejected.
func DecodeStringType(t string) (Any, error) {
	// One scan validates brackets and depth for the whole name, so the
	// recursive parse below does not repeat it at every level.
	if err := checkTypeName(t); err != nil {
		return nil, err
	}
	return decodeStringType(t)
}

func decodeStringType(t string) (Any, error) {
	// checkTypeName ran once for the whole name; arguments can still be empty,
	// as in "Variant(,)".
	if strings.TrimSpace(t) == "" {
		return nil, errors.New("empty type name")
	}

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
		return nil, fmt.Errorf("%w: %s", NotImplementedError, "UInt128")
	case "UInt256":
		return nil, fmt.Errorf("%w: %s", NotImplementedError, "UInt256")
	case "Int8":
		return Int8, nil
	case "Int16":
		return Int16, nil
	case "Int32":
		return Int32, nil
	case "Int64":
		return Int64, nil
	case "Int128":
		return nil, fmt.Errorf("%w: %s", NotImplementedError, "Int128")
	case "Int256":
		return nil, fmt.Errorf("%w: %s", NotImplementedError, "Int256")
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
		return nil, fmt.Errorf("%w: %s", NotImplementedError, "DateTime64")
	case "String":
		return String, nil
	case "FixedString": // <var_uint_size>
		return nil, fmt.Errorf("%w: FixedString without a size", NotImplementedError)
	case "UUID":
		return UUID, nil
	case "IPv4":
		return IPv4, nil
	case "IPv6":
		return IPv6, nil
	case "Dynamic":
		return DynamicDefault, nil
	case "IntervalNanosecond":
		return IntervalNanosecond, nil
	case "IntervalMicrosecond":
		return IntervalMicrosecond, nil
	case "IntervalMillisecond":
		return IntervalMillisecond, nil
	case "IntervalSecond":
		return IntervalSecond, nil
	case "IntervalMinute":
		return IntervalMinute, nil
	case "IntervalHour":
		return IntervalHour, nil
	case "IntervalDay":
		return IntervalDay, nil
	case "IntervalWeek":
		return IntervalWeek, nil
	case "IntervalMonth":
		return IntervalMonth, nil
	case "IntervalQuarter":
		return IntervalQuarter, nil
	case "IntervalYear":
		return IntervalYear, nil
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
		elemType, err := decodeStringType(funcArgs[0])
		if err != nil {
			return nil, err
		}
		return ArrayAny(elemType), nil

	case "Map":
		if len(funcArgs) != 2 {
			return nil, fmt.Errorf("Map must have exactly two arguments: %#v", t)
		}
		keyType, err := decodeStringType(funcArgs[0])
		if err != nil {
			return nil, err
		}
		valueType, err := decodeStringType(funcArgs[1])
		if err != nil {
			return nil, err
		}
		return MapAny(keyType, valueType), nil

	case "Nullable":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("Nullable must have exactly one argument: %#v", t)
		}
		elemType, err := decodeStringType(funcArgs[0])
		if err != nil {
			return nil, err
		}
		return NullableAny(elemType), nil

	case "LowCardinality":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("LowCardinality must have exactly one argument: %#v", t)
		}
		elemType, err := decodeStringType(funcArgs[0])
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
		if err := checkDateTime64Precision(int64(precision)); err != nil {
			return nil, err
		}
		if len(funcArgs) == 1 {
			return DateTime64(uint8(precision)), nil
		}
		return DateTime64TZ(uint8(precision), unquote(funcArgs[1])), nil

	case "Decimal":
		if len(funcArgs) != 2 {
			return nil, fmt.Errorf("Decimal must have exactly two arguments: %#v", t)
		}
		precision, err := parseIntInRange("Decimal precision", funcArgs[0], 1, maxDecimalPrec)
		if err != nil {
			return nil, err
		}
		scale, err := parseIntInRange("Decimal scale", funcArgs[1], 0, precision)
		if err != nil {
			return nil, err
		}
		return Decimal(uint8(precision), uint8(scale)), nil

	case "FixedString":
		if len(funcArgs) != 1 {
			return nil, fmt.Errorf("FixedString must have exactly one argument: %#v", t)
		}
		size, err := parseIntInRange("FixedString size", funcArgs[0], 1, maxFixedStringSize)
		if err != nil {
			return nil, err
		}
		return FixedString(int(size)), nil

	case "Tuple":
		if len(funcArgs) == 0 {
			return TupleAny(), nil // ClickHouse: toTypeName(()) = Tuple(), binary 1f 00
		}

		firstArgArr := decodeStringTypeSplitRoot(funcArgs[0], ' ')

		if len(firstArgArr) == 1 {
			var elemTypes []Any
			for _, arg := range funcArgs {
				elemType, err := decodeStringType(arg)
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
			elemType, err := decodeStringType(argArr[1])
			if err != nil {
				return nil, err
			}
			columns = append(columns, Column{name: argArr[0], tp: elemType})
		}
		return TupleNamedAny(columns...), nil

	case "Enum8":
		// Enum8('windows' = -10, 'android' = 1, 'ios' = 2)
		mp := newEnumBuilder[int8]("Enum8")
		for _, arg := range funcArgs {
			name, valueStr, err := parseEnumElement(arg)
			if err != nil {
				return nil, err
			}
			value, err := parseIntInRange("Enum8 value", valueStr, math.MinInt8, math.MaxInt8)
			if err != nil {
				return nil, err
			}
			if err := mp.add(name, int8(value)); err != nil {
				return nil, err
			}
		}
		return Enum8(mp.labels), nil
	case "Enum16":
		// Enum16('windows' = -10, 'android' = 1, 'ios' = 2)
		mp := newEnumBuilder[int16]("Enum16")
		for _, arg := range funcArgs {
			name, valueStr, err := parseEnumElement(arg)
			if err != nil {
				return nil, err
			}
			value, err := parseIntInRange("Enum16 value", valueStr, math.MinInt16, math.MaxInt16)
			if err != nil {
				return nil, err
			}
			if err := mp.add(name, int16(value)); err != nil {
				return nil, err
			}
		}
		return Enum16(mp.labels), nil

	case "Variant":
		if len(funcArgs) == 0 {
			return nil, fmt.Errorf("Variant must have at least one argument: %#v", t)
		}
		if err := checkVariantCount(uint64(len(funcArgs))); err != nil {
			return nil, err
		}

		var elemTypes []Any
		for _, arg := range funcArgs {
			elemType, err := decodeStringType(arg)
			if err != nil {
				return nil, err
			}
			elemTypes = append(elemTypes, elemType)
		}
		return Variant(elemTypes...), nil

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

		maxTypes, err := parseIntInRange("Dynamic max_types", argArr[1], 0, maxDynamicTypes)
		if err != nil {
			return nil, err
		}
		return Dynamic(uint8(maxTypes)), nil
	}

	if !strings.Contains(t, "(") {
		return Custom(t, Invalid[any]("Unexpected Custom type")), nil
	}

	return nil, fmt.Errorf("can' parse type: %#v", t)
}
