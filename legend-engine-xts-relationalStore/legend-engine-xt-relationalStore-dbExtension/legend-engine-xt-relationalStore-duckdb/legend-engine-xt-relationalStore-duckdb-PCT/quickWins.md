# DuckDB PCT Quick Wins - Test Failures to Fix

This document lists all the test failures that are considered "Quick Wins" - relatively straightforward fixes that would significantly reduce the overall failure count.

**Total Quick Win Tests: 31**
- Index Offset Issues: 5 tests
- DateTime Format Issues: 15 tests  
- Decimal Precision Issues: 11 tests

---

## 1. Fix Index Offset Issues (5 tests)

**Issue:** 0-indexed (Pure) vs 1-indexed (SQL standard) mismatches

**Root Cause:** Pure uses 0-based indexing while SQL standard uses 1-based indexing. Need to adjust translations accordingly.

### Tests Failing:

| # | Test Name | Expected | Actual | Test Suite |
|---|-----------|----------|--------|------------|
| 1 | `meta::pure::functions::collection::tests::indexof::testIndexOfOneElement_Function_1__Boolean_1_` | 0 | 1 | EssentialFunctions |
| 2 | `meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_Function_1__Boolean_1_` | 3 | 4 | UnclassifiedFunctions |
| 3 | `meta::pure::functions::string::tests::regexpIndexOf::testRegexpIndexOf_GroupNumber_Function_1__Boolean_1_` | 3 | 4 | UnclassifiedFunctions |
| 4 | `meta::pure::functions::string::tests::substring::testStartEnd_Function_1__Boolean_1_` | 'the quick brown fox jumps over the lazy dog' | 'the quick brown fox jumps over the lazy do' | EssentialFunctions |
| 5 | `meta::pure::functions::string::tests::substring::testStart_Function_1__Boolean_1_` | 'he quick brown fox jumps over the lazy dog' | 'the quick brown fox jumps over the lazy dog' | EssentialFunctions |

**Fix Strategy:**
- Adjust `indexOf` function to subtract 1 from SQL result
- Adjust `regexpIndexOf` function to subtract 1 from SQL result
- Adjust `substring` function to add 1 to start position when translating to SQL

---

## 2. Fix DateTime Format Output (15 tests)

**Issue:** DateTime format mismatches - nanoseconds vs milliseconds precision, and missing ISO8601 formatting with timezone

**Root Cause:** DuckDB returns timestamps with nanosecond precision and different format string. Pure expects millisecond precision with ISO8601 format including timezone.

### Tests Failing:

| # | Test Name | Expected Format | Actual Format | Test Suite |
|---|-----------|-----------------|---------------|------------|
| 1 | `meta::pure::functions::variant::convert::tests::to::testToDateTime_Function_1__Boolean_1_` | `%2020-01-01T01:01:00.000+0000` | `%2020-01-01T01:01:00.000000000+0000` | VariantFunctions |
| 2 | `meta::pure::functions::collection::tests::greatest::testGreatest_DateTime_Function_1__Boolean_1_` | `%2025-02-10T20:10:20+0000` | `%2025-02-10T20:10:20.000000000+0000` | StandardFunctions |
| 3 | `meta::pure::functions::collection::tests::greatest::testGreatest_Single_Function_1__Boolean_1_` | `%2025-02-10T20:10:20+0000` | `%2025-02-10T20:10:20.000000000+0000` | StandardFunctions |
| 4 | `meta::pure::functions::collection::tests::least::testLeast_DateTime_Function_1__Boolean_1_` | `%2025-01-10T15:25:30+0000` | `%2025-01-10T15:25:30.000000000+0000` | StandardFunctions |
| 5 | `meta::pure::functions::collection::tests::least::testLeast_Single_Function_1__Boolean_1_` | `%2025-02-10T20:10:20+0000` | `%2025-02-10T20:10:20.000000000+0000` | StandardFunctions |
| 6 | `meta::pure::functions::string::tests::toString::testDateTimeToString_Function_1__Boolean_1_` | `'2014-01-01T00:00:00.000+0000'` | `'2014-01-01 00:00:00'` | EssentialFunctions |
| 7 | `meta::pure::functions::string::tests::toString::testDateTimeWithTimezoneToString_Function_1__Boolean_1_` | `'2014-01-01T00:00:00.0000+0000'` | `'2014-01-01 00:00:00'` | EssentialFunctions |
| 8 | `meta::pure::functions::string::tests::toString::testSimpleDateToString_Function_1__Boolean_1_` | `'2014-01-02T01:54:27.352+0000'` | `'2014-01-02 01:54:27.352'` | EssentialFunctions |
| 9 | `meta::pure::functions::date::tests::testDateFromSecond_Function_1__Boolean_1_` | `%1973-11-13T23:09:11+0000` | `%1973-11-13T23:09:11.000000000+0000` | EssentialFunctions |
| 10 | `meta::pure::functions::date::tests::testDateFromSubSecond_Function_1__Boolean_1_` | `%1973-11-13T23:09:11.0+0000` | `%1973-11-13T23:09:11.000000000+0000` | EssentialFunctions |
| 11 | `meta::pure::functions::date::tests::testAdjustByMicrosecondsBigNumber_Function_1__Boolean_1_` | `%2021-06-21T09:37:37.4990000+0000` | `%2021-06-21T09:37:37.499+0000` | EssentialFunctions |

### Additional DateTime-Related Tests (4 tests):

These appear to be multiline regexp parameter issues, not directly datetime format:

| # | Test Name | Test Suite |
|---|-----------|------------|
| 12 | `meta::pure::functions::string::tests::regexpLike::testRegexpLike_CaseInsensitive_Multiline_Function_1__Boolean_1_` | UnclassifiedFunctions |
| 13 | `meta::pure::functions::string::tests::regexpLike::testRegexpLike_CaseInsensitive_Multiline_NonNewlineSensitive_Function_1__Boolean_1_` | UnclassifiedFunctions |
| 14 | `meta::pure::functions::string::tests::regexpLike::testRegexpLike_Multiline_Function_1__Boolean_1_` | UnclassifiedFunctions |
| 15 | `meta::pure::functions::string::tests::regexpLike::testRegexpLike_Multiline_NonNewlineSensitive_Function_1__Boolean_1_` | UnclassifiedFunctions |

**Fix Strategy:**
- Standardize datetime output to use ISO8601 format: `YYYY-MM-DDTHH:MM:SS.sss+ZZZZ`
- Truncate nanosecond precision to millisecond (3 decimal places instead of 9)
- Use `strftime` or `format` functions in DuckDB to control output format
- Ensure timezone is always included as `+0000` for UTC

---

## 3. Fix Decimal Precision Representation (11 tests)

**Issue:** Decimal 'D' suffix and trailing zero precision mismatches

**Root Cause:** DuckDB returns decimals without the 'D' suffix that Pure expects, and may strip trailing zeros from decimal representations.

### Tests Failing:

| # | Test Name | Expected | Actual | Test Suite |
|---|-----------|----------|--------|------------|
| 1 | `meta::pure::functions::math::tests::rem::testRemWithDecimals_Function_1__Boolean_1_` | `0.14D` | `0.14` | EssentialFunctions |
| 2 | `meta::pure::functions::math::tests::toDecimal::testIntToDecimal_Function_1__Boolean_1_` | `8D` | `8.0D` | EssentialFunctions |
| 3 | `meta::pure::functions::math::tests::max::testMax_Numbers_Function_1__Boolean_1_` | `2` | `2.0` | StandardFunctions |
| 4 | `meta::pure::functions::collection::tests::greatest::testGreatest_Number_Function_1__Boolean_1_` | `2` | `2.0` | StandardFunctions |
| 5 | `meta::pure::functions::math::tests::min::testMin_Numbers_Function_1__Boolean_1_` | `1.23D` | `1.23` | StandardFunctions |
| 6 | `meta::pure::functions::collection::tests::least::testLeast_Number_Function_1__Boolean_1_` | `1.0D` | `1.0` | StandardFunctions |
| 7 | `meta::pure::functions::math::tests::minus::testDecimalMinus_Function_1__Boolean_1_` | `-4.0D` | `-4.0` | GrammarFunctions |
| 8 | `meta::pure::functions::math::tests::plus::testDecimalPlus_Function_1__Boolean_1_` | `6.0D` | `6.0` | GrammarFunctions |
| 9 | `meta::pure::functions::math::tests::times::testDecimalTimes_Function_1__Boolean_1_` | `353791.470D` | `353791.47` | GrammarFunctions |
| 10 | `meta::pure::functions::string::tests::parseDecimal::testParseDecimalWithPrecisionScale_Function_1__Boolean_1_` | `123.12300D` | `123.123D` | EssentialFunctions |
| 11 | `meta::pure::functions::string::tests::parseDecimal::testParseZero_Function_1__Boolean_1_` | `0.000D` | `0.0D` | EssentialFunctions |

**Fix Strategy:**
- Ensure decimal values include 'D' suffix in string representation
- Preserve trailing zeros to match expected precision
- May need to use `format()` or `printf()` functions to control decimal output format
- For `toDecimal` with precision/scale, ensure output matches the specified scale

---

## Implementation Notes

### Priority Order:
1. **Decimal Precision** (11 tests) - Likely the easiest fix, may just need to adjust string formatting
2. **Index Offset** (5 tests) - Simple arithmetic adjustment in translation layer
3. **DateTime Format** (15 tests) - May require more complex string formatting logic

### Files to Modify:
- `/legend-engine-xt-relationalStore-duckdb-sqlDialectTranslation-pure/` - SQL translation functions
- `/legend-engine-xt-relationalStore-duckdb-pure/` - DuckDB-specific Pure extensions

### Testing:
After implementing fixes, run the specific test suites to verify:
```bash
mv