export enum OpenSearchIndex {
  MEMBERS = 'members',
  ORGANIZATIONS = 'organizations',
}

// Keeps track of version numbers for all OpenSearch indexes, aiding in managing documents.
// for eg: members_v1, activities_v1, etc.
export const IndexVersions = new Map<OpenSearchIndex, number>()
IndexVersions.set(OpenSearchIndex.MEMBERS, 5)
IndexVersions.set(OpenSearchIndex.ORGANIZATIONS, 5)

const prefixedMapping = {
  dynamic_templates: [
    // https://opensearch.org/docs/latest/field-types/supported-field-types/string/
    {
      strings: {
        match_pattern: 'regex',
        match: '^string_.*',
        path_match: '.*',
        mapping: {
          type: 'text',
          analyzer: 'lowercase_keyword_analyzer',
        },
      },
    },
    {
      nox_strings: {
        match_pattern: 'regex',
        match: '^nox_string_.*',
        path_match: '.*',
        mapping: {
          type: 'text',
          index: false,
        },
      },
    },
    {
      uuids: {
        match_pattern: 'regex',
        match: '^uuid_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
        },
      },
    },
    {
      nox_uuids: {
        match_pattern: 'regex',
        match: '^nox_uuid_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
          index: false,
        },
      },
    },
    {
      keywords: {
        match_pattern: 'regex',
        match: '^keyword_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
          normalizer: 'lowercase_normalizer',
        },
      },
    },
    {
      nox_keywords: {
        match_pattern: 'regex',
        match: '^nox_keyword_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
          index: false,
        },
      },
    },
    // https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
    {
      integers: {
        match_pattern: 'regex',
        match: '^int_.*',
        path_match: '.*',
        mapping: {
          type: 'integer',
        },
      },
    },
    {
      nox_integers: {
        match_pattern: 'regex',
        match: '^nox_int_.*',
        path_match: '.*',
        mapping: {
          type: 'integer',
          index: false,
        },
      },
    },
    {
      floats: {
        match_pattern: 'regex',
        match: '^float_.*',
        path_match: '.*',
        mapping: {
          type: 'float',
        },
      },
    },
    {
      nox_floats: {
        match_pattern: 'regex',
        match: '^nox_float_.*',
        path_match: '.*',
        mapping: {
          type: 'float',
          index: false,
        },
      },
    },
    {
      bytes: {
        match_pattern: 'regex',
        match: '^byte_.*',
        path_match: '.*',
        mapping: {
          type: 'byte',
        },
      },
    },
    {
      nox_bytes: {
        match_pattern: 'regex',
        match: '^nox_byte_.*',
        path_match: '.*',
        mapping: {
          type: 'byte',
          index: false,
        },
      },
    },
    {
      doubles: {
        match_pattern: 'regex',
        match: '^double_.*',
        path_match: '.*',
        mapping: {
          type: 'double',
        },
      },
    },
    {
      nox_doubles: {
        match_pattern: 'regex',
        match: '^nox_double_.*',
        path_match: '.*',
        mapping: {
          type: 'double',
          index: false,
        },
      },
    },
    {
      half_floats: {
        match_pattern: 'regex',
        match: '^half_float_.*',
        path_match: '.*',
        mapping: {
          type: 'half_float',
        },
      },
    },
    {
      nox_half_floats: {
        match_pattern: 'regex',
        match: '^nox_half_float_.*',
        path_match: '.*',
        mapping: {
          type: 'half_float',
          index: false,
        },
      },
    },
    {
      longs: {
        match_pattern: 'regex',
        match: '^long_.*',
        path_match: '.*',
        mapping: {
          type: 'long',
        },
      },
    },
    {
      nox_longs: {
        match_pattern: 'regex',
        match: '^nox_long_.*',
        path_match: '.*',
        mapping: {
          type: 'long',
          index: false,
        },
      },
    },
    {
      shorts: {
        match_pattern: 'regex',
        match: '^short_.*',
        path_match: '.*',
        mapping: {
          type: 'short',
        },
      },
    },
    {
      nox_shorts: {
        match_pattern: 'regex',
        match: '^nox_short_.*',
        path_match: '.*',
        mapping: {
          type: 'short',
          index: false,
        },
      },
    },
    // https://opensearch.org/docs/latest/field-types/supported-field-types/date/
    {
      dates: {
        match_pattern: 'regex',
        match: '^date_.*',
        path_match: '.*',
        mapping: {
          type: 'date',
          format:
            'strict_date_optional_time||strict_date_optional_time_nanos||epoch_millis||yyyy-MM-dd HH:mm:ssXXX||yyyy-MM-dd HH:mm:ssX||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SSSZ',
        },
      },
    },
    {
      nox_dates: {
        match_pattern: 'regex',
        match: '^nox_date_.*',
        path_match: '.*',
        mapping: {
          type: 'date',
          format:
            'strict_date_optional_time||strict_date_optional_time_nanos||epoch_millis||yyyy-MM-dd HH:mm:ssXXX||yyyy-MM-dd HH:mm:ssX||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SSSZ',
          index: false,
        },
      },
    },
    // https://opensearch.org/docs/latest/field-types/supported-field-types/boolean/
    {
      booleans: {
        match_pattern: 'regex',
        match: '^bool_.*',
        path_match: '.*',
        mapping: {
          type: 'boolean',
        },
      },
    },
    {
      nox_booleans: {
        match_pattern: 'regex',
        match: '^nox_bool_.*',
        path_match: '.*',
        mapping: {
          type: 'boolean',
          index: false,
        },
      },
    },
    // https://opensearch.org/docs/latest/field-types/supported-field-types/object-fields/
    {
      objects: {
        match_pattern: 'regex',
        match: '^obj_.*',
        path_match: '.*',
        mapping: {
          type: 'object',
        },
      },
    },
    {
      nox_objects: {
        match_pattern: 'regex',
        match: '^nox_obj_.*',
        path_match: '.*',
        mapping: {
          type: 'object',
          index: false,
        },
      },
    },
    {
      nested_objects: {
        match_pattern: 'regex',
        match: '^nested_.*',
        path_match: '.*',
        mapping: {
          type: 'nested',
        },
      },
    },
    {
      nox_nested_objects: {
        match_pattern: 'regex',
        match: '^nox_nested_.*',
        path_match: '.*',
        mapping: {
          type: 'nested',
          index: false,
        },
      },
    },
    {
      string_arrays: {
        match_pattern: 'regex',
        match: '^string_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'text',
        },
      },
    },
    {
      nox_string_arrays: {
        match_pattern: 'regex',
        match: '^nox_string_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'text',
          index: false,
        },
      },
    },
    {
      integer_arrays: {
        match_pattern: 'regex',
        match: '^int_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'integer',
        },
      },
    },
    {
      nox_integer_arrays: {
        match_pattern: 'regex',
        match: '^nox_int_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'integer',
          index: false,
        },
      },
    },
    {
      float_arrays: {
        match_pattern: 'regex',
        match: '^float_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'float',
        },
      },
    },
    {
      nox_float_arrays: {
        match_pattern: 'regex',
        match: '^nox_float_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'float',
          index: false,
        },
      },
    },
    {
      byte_arrays: {
        match_pattern: 'regex',
        match: '^byte_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'byte',
        },
      },
    },
    {
      nox_byte_arrays: {
        match_pattern: 'regex',
        match: '^nox_byte_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'byte',
          index: false,
        },
      },
    },
    {
      double_arrays: {
        match_pattern: 'regex',
        match: '^double_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'double',
        },
      },
    },
    {
      nox_double_arrays: {
        match_pattern: 'regex',
        match: '^nox_double_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'double',
          index: false,
        },
      },
    },
    {
      half_float_arrays: {
        match_pattern: 'regex',
        match: '^half_float_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'half_float',
        },
      },
    },
    {
      nox_half_float_arrays: {
        match_pattern: 'regex',
        match: '^nox_half_float_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'half_float',
          index: false,
        },
      },
    },
    {
      long_arrays: {
        match_pattern: 'regex',
        match: '^long_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'long',
        },
      },
    },
    {
      nox_long_arrays: {
        match_pattern: 'regex',
        match: '^nox_long_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'long',
          index: false,
        },
      },
    },
    {
      short_arrays: {
        match_pattern: 'regex',
        match: '^short_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'short',
        },
      },
    },
    {
      nox_short_arrays: {
        match_pattern: 'regex',
        match: '^nox_short_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'short',
          index: false,
        },
      },
    },
    {
      date_arrays: {
        match_pattern: 'regex',
        match: '^date_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'date',
          format:
            'strict_date_optional_time||strict_date_optional_time_nanos||epoch_millis||yyyy-MM-dd HH:mm:ssXXX||yyyy-MM-dd HH:mm:ssX||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SSSZ',
        },
      },
    },
    {
      nox_date_arrays: {
        match_pattern: 'regex',
        match: '^nox_date_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'date',
          format:
            'strict_date_optional_time||strict_date_optional_time_nanos||epoch_millis||yyyy-MM-dd HH:mm:ssXXX||yyyy-MM-dd HH:mm:ssX||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SSSZ',
          index: false,
        },
      },
    },
    {
      boolean_arrays: {
        match_pattern: 'regex',
        match: '^bool_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'boolean',
        },
      },
    },
    {
      nox_boolean_arrays: {
        match_pattern: 'regex',
        match: '^nox_bool_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'boolean',
          index: false,
        },
      },
    },
    {
      uuid_arrays: {
        match_pattern: 'regex',
        match: '^uuid_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
        },
      },
    },
    {
      nox_uuid_arrays: {
        match_pattern: 'regex',
        match: '^nox_uuid_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
          index: false,
        },
      },
    },
    {
      keyword_arrays: {
        match_pattern: 'regex',
        match: '^keyword_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
        },
      },
    },
    {
      nox_keyword_arrays: {
        match_pattern: 'regex',
        match: '^nox_keyword_arr_.*',
        path_match: '.*',
        mapping: {
          type: 'keyword',
          index: false,
        },
      },
    },
  ],
}

const prefixedSettings = {
  analysis: {
    analyzer: {
      lowercase_keyword_analyzer: {
        type: 'custom',
        tokenizer: 'keyword',
        filter: ['lowercase'],
      },
    },
    normalizer: {
      lowercase_normalizer: {
        type: 'custom',
        filter: ['lowercase'],
      },
    },
  },
}

export const OPENSEARCH_INDEX_SETTINGS: Record<OpenSearchIndex, unknown> = {
  [OpenSearchIndex.MEMBERS]: prefixedSettings,
  [OpenSearchIndex.ORGANIZATIONS]: prefixedSettings,
}

export const OPENSEARCH_INDEX_MAPPINGS: Record<OpenSearchIndex, unknown> = {
  [OpenSearchIndex.MEMBERS]: prefixedMapping,
  [OpenSearchIndex.ORGANIZATIONS]: prefixedMapping,
}
