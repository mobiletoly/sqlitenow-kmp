final class RichNumericScenario {
  const RichNumericScenario({
    required this.name,
    required this.local,
    required this.committed,
  });

  final String name;
  final Map<String, String> local;
  final Map<String, String> committed;

  Map<String, Object> toJson() => {
    'name': name,
    'local': local,
    'committed': committed,
  };
}

const richNumericScenarios = <RichNumericScenario>[
  RichNumericScenario(
    name: 'signed-64-min',
    local: {
      'count_value': '-9223372036854775808',
      'small_count': '-32768',
      'medium_count': '-2147483648',
      'exact_amount': '-1234567890.123456789',
    },
    committed: {
      'count_value': '-9223372036854775808',
      'small_count': '-32768',
      'medium_count': '-2147483648',
      'exact_amount': '-1234567890.1234567890',
    },
  ),
  RichNumericScenario(
    name: 'signed-64-max',
    local: {
      'count_value': '9223372036854775807',
      'small_count': '32767',
      'medium_count': '2147483647',
      'exact_amount': '1234567890.123456789',
    },
    committed: {
      'count_value': '9223372036854775807',
      'small_count': '32767',
      'medium_count': '2147483647',
      'exact_amount': '1234567890.1234567890',
    },
  ),
  RichNumericScenario(
    name: 'above-javascript-safe-range',
    local: {'count_value': '9007199254740993'},
    committed: {'count_value': '9007199254740993'},
  ),
  RichNumericScenario(
    name: 'binary64-negative-zero',
    local: {'rating': '-0.0'},
    committed: {'rating': '0'},
  ),
  RichNumericScenario(
    name: 'binary64-subnormal',
    local: {'rating': '5e-324'},
    committed: {'rating': '5e-324'},
  ),
  RichNumericScenario(
    name: 'binary64-ordinary',
    local: {'rating': '6.57111473696007'},
    committed: {'rating': '6.57111473696007'},
  ),
  RichNumericScenario(
    name: 'binary64-maximum-finite',
    local: {'rating': '1.7976931348623157e+308'},
    committed: {'rating': '1.7976931348623157e+308'},
  ),
  RichNumericScenario(
    name: 'postgres-float4-authoritative-spelling',
    local: {'float4_value': '1.2345678901234567'},
    committed: {'float4_value': '1.2345678806304932'},
  ),
  RichNumericScenario(
    name: 'boolean-false',
    local: {'enabled_flag': '0'},
    committed: {'enabled_flag': 'false'},
  ),
  RichNumericScenario(
    name: 'boolean-true',
    local: {'enabled_flag': '1'},
    committed: {'enabled_flag': 'true'},
  ),
];
