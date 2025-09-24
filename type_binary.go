package rowbinary

// https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding

var (
	typeBinaryNothing                 = [1]byte{0x00}
	typeBinaryUInt8                   = [1]byte{0x01}
	typeBinaryUInt16                  = [1]byte{0x02}
	typeBinaryUInt32                  = [1]byte{0x03}
	typeBinaryUInt64                  = [1]byte{0x04}
	typeBinaryUInt128                 = [1]byte{0x05}
	typeBinaryUInt256                 = [1]byte{0x06}
	typeBinaryInt8                    = [1]byte{0x07}
	typeBinaryInt16                   = [1]byte{0x08}
	typeBinaryInt32                   = [1]byte{0x09}
	typeBinaryInt64                   = [1]byte{0x0A}
	typeBinaryInt128                  = [1]byte{0x0B}
	typeBinaryInt256                  = [1]byte{0x0C}
	typeBinaryFloat32                 = [1]byte{0x0D}
	typeBinaryFloat64                 = [1]byte{0x0E}
	typeBinaryDate                    = [1]byte{0x0F}
	typeBinaryDate32                  = [1]byte{0x10}
	typeBinaryDateTime                = [1]byte{0x11}
	typeBinaryDateTimeWithTimeZone    = [1]byte{0x12} // <var_uint_time_zone_name_size><time_zone_name_data>
	typeBinaryDateTime64              = [1]byte{0x13} // <uint8_precision>
	typeBinaryDateTime64WithTimeZone  = [1]byte{0x14} // <uint8_precision><var_uint_time_zone_name_size><time_zone_name_data>
	typeBinaryString                  = [1]byte{0x15}
	typeBinaryFixedString             = [1]byte{0x16} // <var_uint_size>
	typeBinaryEnum8                   = [1]byte{0x17} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int8_value_1>...<var_uint_name_size_N><name_data_N><int8_value_N>
	typeBinaryEnum16                  = [1]byte{0x18} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int16_little_endian_value_1>...><var_uint_name_size_N><name_data_N><int16_little_endian_value_N>
	typeBinaryDecimal32               = [1]byte{0x19} // <uint8_precision><uint8_scale>
	typeBinaryDecimal64               = [1]byte{0x1A} // <uint8_precision><uint8_scale>
	typeBinaryDecimal128              = [1]byte{0x1B} // <uint8_precision><uint8_scale>
	typeBinaryDecimal256              = [1]byte{0x1C} // <uint8_precision><uint8_scale>
	typeBinaryUUID                    = [1]byte{0x1D}
	typeBinaryArray                   = [1]byte{0x1E} // <nested_type_encoding>
	typeBinaryTuple                   = [1]byte{0x1F} // <var_uint_number_of_elements><nested_type_encoding_1>...<nested_type_encoding_N>
	typeBinaryTupleNamed              = [1]byte{0x20} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
	typeBinarySet                     = [1]byte{0x21}
	typeBinaryInterval                = [1]byte{0x22} // <interval_kind> (see interval kind binary encoding)
	typeBinaryNullable                = [1]byte{0x23} // <nested_type_encoding>
	typeBinaryFunction                = [1]byte{0x24} // <var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N><return_type_encoding>
	typeBinaryAggregateFunction       = [1]byte{0x25} // <var_uint_version><var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)
	typeBinaryLowCardinality          = [1]byte{0x26} // <nested_type_encoding>
	typeBinaryMap                     = [1]byte{0x27} // <key_type_encoding><value_type_encoding>
	typeBinaryIPv4                    = [1]byte{0x28}
	typeBinaryIPv6                    = [1]byte{0x29}
	typeBinaryVariant                 = [1]byte{0x2A} // <var_uint_number_of_variants><variant_type_encoding_1>...<variant_type_encoding_N>
	typeBinaryDynamic                 = [1]byte{0x2B} // <uint8_max_types>
	typeBinaryCustom                  = [1]byte{0x2C} // <var_uint_type_name_size><type_name_data>
	typeBinaryBool                    = [1]byte{0x2D}
	typeBinarySimpleAggregateFunction = [1]byte{0x2E} // <var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)
	typeBinaryNested                  = [1]byte{0x2F} // <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
	typeBinaryJSON                    = [1]byte{0x30} // <uint8_serialization_version><var_int_max_dynamic_paths><uint8_max_dynamic_types><var_uint_number_of_typed_paths><var_uint_path_name_size_1><path_name_data_1><encoded_type_1>...<var_uint_number_of_skip_paths><var_uint_skip_path_size_1><skip_path_data_1>...<var_uint_number_of_skip_path_regexps><var_uint_skip_path_regexp_size_1><skip_path_data_regexp_1>...
	typeBinaryBFloat16                = [1]byte{0x31}
	typeBinaryTime                    = [1]byte{0x32}
	typeBinaryTime64                  = [1]byte{0x34} // <uint8_precision>
)
