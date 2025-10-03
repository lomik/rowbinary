package rowbinary

import (
	"encoding/binary"
	"errors"
	"io"
)

// https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding

var (
	BinaryTypeNothing                 = [1]byte{0x00}
	BinaryTypeUInt8                   = [1]byte{0x01}
	BinaryTypeUInt16                  = [1]byte{0x02}
	BinaryTypeUInt32                  = [1]byte{0x03}
	BinaryTypeUInt64                  = [1]byte{0x04}
	BinaryTypeUInt128                 = [1]byte{0x05}
	BinaryTypeUInt256                 = [1]byte{0x06}
	BinaryTypeInt8                    = [1]byte{0x07}
	BinaryTypeInt16                   = [1]byte{0x08}
	BinaryTypeInt32                   = [1]byte{0x09}
	BinaryTypeInt64                   = [1]byte{0x0A}
	BinaryTypeInt128                  = [1]byte{0x0B}
	BinaryTypeInt256                  = [1]byte{0x0C}
	BinaryTypeFloat32                 = [1]byte{0x0D}
	BinaryTypeFloat64                 = [1]byte{0x0E}
	BinaryTypeDate                    = [1]byte{0x0F}
	BinaryTypeDate32                  = [1]byte{0x10}
	BinaryTypeDateTime                = [1]byte{0x11}
	BinaryTypeDateTimeWithTimeZone    = [1]byte{0x12} // <var_uint_time_zone_name_size><time_zone_name_data>
	BinaryTypeDateTime64              = [1]byte{0x13} // <uint8_precision>
	BinaryTypeDateTime64WithTimeZone  = [1]byte{0x14} // <uint8_precision><var_uint_time_zone_name_size><time_zone_name_data>
	BinaryTypeString                  = [1]byte{0x15}
	BinaryTypeFixedString             = [1]byte{0x16} // <var_uint_size>
	BinaryTypeEnum8                   = [1]byte{0x17} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int8_value_1>...<var_uint_name_size_N><name_data_N><int8_value_N>
	BinaryTypeEnum16                  = [1]byte{0x18} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int16_little_endian_value_1>...><var_uint_name_size_N><name_data_N><int16_little_endian_value_N>
	BinaryTypeDecimal32               = [1]byte{0x19} // <uint8_precision><uint8_scale>
	BinaryTypeDecimal64               = [1]byte{0x1A} // <uint8_precision><uint8_scale>
	BinaryTypeDecimal128              = [1]byte{0x1B} // <uint8_precision><uint8_scale>
	BinaryTypeDecimal256              = [1]byte{0x1C} // <uint8_precision><uint8_scale>
	BinaryTypeUUID                    = [1]byte{0x1D}
	BinaryTypeArray                   = [1]byte{0x1E} // <nested_type_encoding>
	BinaryTypeTuple                   = [1]byte{0x1F} // <var_uint_number_of_elements><nested_type_encoding_1>...<nested_type_encoding_N>
	BinaryTypeTupleNamed              = [1]byte{0x20} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
	BinaryTypeSet                     = [1]byte{0x21}
	BinaryTypeInterval                = [1]byte{0x22} // <interval_kind> (see interval kind binary encoding)
	BinaryTypeNullable                = [1]byte{0x23} // <nested_type_encoding>
	BinaryTypeFunction                = [1]byte{0x24} // <var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N><return_type_encoding>
	BinaryTypeAggregateFunction       = [1]byte{0x25} // <var_uint_version><var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)
	BinaryTypeLowCardinality          = [1]byte{0x26} // <nested_type_encoding>
	BinaryTypeMap                     = [1]byte{0x27} // <key_type_encoding><value_type_encoding>
	BinaryTypeIPv4                    = [1]byte{0x28}
	BinaryTypeIPv6                    = [1]byte{0x29}
	BinaryTypeVariant                 = [1]byte{0x2A} // <var_uint_number_of_variants><variant_type_encoding_1>...<variant_type_encoding_N>
	BinaryTypeDynamic                 = [1]byte{0x2B} // <uint8_max_types>
	BinaryTypeCustom                  = [1]byte{0x2C} // <var_uint_type_name_size><type_name_data>
	BinaryTypeBool                    = [1]byte{0x2D}
	BinaryTypeSimpleAggregateFunction = [1]byte{0x2E} // <var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)
	BinaryTypeNested                  = [1]byte{0x2F} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
	BinaryTypeJSON                    = [1]byte{0x30} // <uint8_serialization_version><var_int_max_dynamic_paths><uint8_max_dynamic_types><var_uint_number_of_typed_paths><var_uint_path_name_size_1><path_name_data_1><encoded_type_1>...<var_uint_number_of_skip_paths><var_uint_skip_path_size_1><skip_path_data_1>...<var_uint_number_of_skip_path_regexps><var_uint_skip_path_regexp_size_1><skip_path_data_regexp_1>...
	BinaryTypeBFloat16                = [1]byte{0x31}
	BinaryTypeTime                    = [1]byte{0x32}
	BinaryTypeTime64                  = [1]byte{0x34} // <uint8_precision>
)

// DecodeBinaryType decodes a binary type from the given reader.
func DecodeBinaryType(r Reader) (Any, error) {
	var firstByte [1]byte
	if _, err := io.ReadFull(r, firstByte[:]); err != nil {
		return nil, err
	}

	switch firstByte {
	case BinaryTypeNothing:
		return Nothing, nil
	case BinaryTypeUInt8:
		return UInt8, nil
	case BinaryTypeUInt16:
		return UInt16, nil
	case BinaryTypeUInt32:
		return UInt32, nil
	case BinaryTypeUInt64:
		return UInt64, nil
	case BinaryTypeUInt128:
		return nil, errors.New("not implemented")
	case BinaryTypeUInt256:
		return nil, errors.New("not implemented")
	case BinaryTypeInt8:
		return Int8, nil
	case BinaryTypeInt16:
		return Int16, nil
	case BinaryTypeInt32:
		return Int32, nil
	case BinaryTypeInt64:
		return Int64, nil
	case BinaryTypeInt128:
		return nil, errors.New("not implemented")
	case BinaryTypeInt256:
		return nil, errors.New("not implemented")
	case BinaryTypeFloat32:
		return Float32, nil
	case BinaryTypeFloat64:
		return Float64, nil
	case BinaryTypeDate:
		return Date, nil
	case BinaryTypeDate32:
		return Date32, nil
	case BinaryTypeDateTime:
		return DateTime, nil
	case BinaryTypeDateTimeWithTimeZone:
		return nil, errors.New("not implemented")
	case BinaryTypeDateTime64:
		return nil, errors.New("not implemented")
	case BinaryTypeDateTime64WithTimeZone:
		return nil, errors.New("not implemented")
	case BinaryTypeString:
		return String, nil
	case BinaryTypeFixedString: // <var_uint_size>
		size, err := UVarint.Read(r)
		if err != nil {
			return nil, err
		}
		return FixedString(int(size)), nil
	case BinaryTypeEnum8:
		return nil, errors.New("not implemented")
	case BinaryTypeEnum16:
		return nil, errors.New("not implemented")
	case BinaryTypeDecimal32, BinaryTypeDecimal64, BinaryTypeDecimal128, BinaryTypeDecimal256: // <uint8_precision><uint8_scale>
		precision, err := UInt8.Read(r)
		if err != nil {
			return nil, err
		}
		scale, err := UInt8.Read(r)
		if err != nil {
			return nil, err
		}
		return Decimal(precision, scale), nil
	case BinaryTypeUUID:
		return UUID, nil
	case BinaryTypeArray:
		nested, err := DecodeBinaryType(r)
		if err != nil {
			return nil, err
		}
		return ArrayAny(nested), nil
	case BinaryTypeTuple:
		n, err := binary.ReadUvarint(r)
		types := make([]Any, 0, n)
		if err != nil {
			return nil, err
		}
		for range n {
			tp, err := DecodeBinaryType(r)
			if err != nil {
				return nil, err
			}
			types = append(types, tp)
		}
		return TupleAny(types...), nil
	case BinaryTypeTupleNamed: // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		columns := make([]Column, 0, n)
		for i := 0; i < int(n); i++ {
			name, err := String.Read(r)
			if err != nil {
				return nil, err
			}
			tp, err := DecodeBinaryType(r)
			if err != nil {
				return nil, err
			}
			columns = append(columns, Column{name: name, tp: tp})
		}
		return TupleNamedAny(columns...), nil
	case BinaryTypeSet:
		return nil, errors.New("not implemented")
	case BinaryTypeInterval:
		return nil, errors.New("not implemented")
	case BinaryTypeNullable: // <nested_type_encoding>
		nested, err := DecodeBinaryType(r)
		if err != nil {
			return nil, err
		}
		return NullableAny(nested), nil
	case BinaryTypeFunction:
		return nil, errors.New("not implemented")
	case BinaryTypeAggregateFunction:
		return nil, errors.New("not implemented")
	case BinaryTypeLowCardinality: // <nested_type_encoding>
		nested, err := DecodeBinaryType(r)
		if err != nil {
			return nil, err
		}
		return LowCardinalityAny(nested), nil
	case BinaryTypeMap: // <key_type_encoding><value_type_encoding>
		keyType, err := DecodeBinaryType(r)
		if err != nil {
			return nil, err
		}
		valueType, err := DecodeBinaryType(r)
		if err != nil {
			return nil, err
		}
		return MapAny(keyType, valueType), nil
	case BinaryTypeIPv4:
		return nil, errors.New("not implemented")
	case BinaryTypeIPv6:
		return nil, errors.New("not implemented")
	case BinaryTypeVariant:
		return nil, errors.New("not implemented")
	case BinaryTypeDynamic:
		return nil, errors.New("not implemented")
	case BinaryTypeCustom:
		return nil, errors.New("not implemented")
	case BinaryTypeBool:
		return Bool, nil
	case BinaryTypeSimpleAggregateFunction:
		return nil, errors.New("not implemented")
	case BinaryTypeNested:
		return nil, errors.New("not implemented")
	case BinaryTypeJSON:
		return nil, errors.New("not implemented")
	case BinaryTypeBFloat16:
		return nil, errors.New("not implemented")
	case BinaryTypeTime:
		return nil, errors.New("not implemented")
	case BinaryTypeTime64:
		return nil, errors.New("not implemented")
	default:
		return nil, errors.New("not implemented")
	}

}
