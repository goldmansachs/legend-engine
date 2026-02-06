# DuckDB PCT Test Failures Summary

## Overview

| Test Suite | Number of Failures |
|------------|-------------------|
| EssentialFunctions | 111 |
| RelationFunctions | 1 |
| StandardFunctions | 35 |
| GrammarFunctions | 32 |
| UnclassifiedFunctions | 12 |
| VariantFunctions | 16 |
| ScenarioQuantFunctions | 0 |
| **TOTAL** | **207** |

---

## Failures by Category/Root Cause

### 1. Unsupported API Functions (needsImplementation)

These functions need SQL translation implementation for DuckDB:

| Function | Count | Error Pattern |
|----------|-------|---------------|
| `forAll` | 3 | `No SQL translation exists for 'forAll_T_MANY__Function_1__Boolean_1_'` |
| `sort` (with functions) | 4 | `No SQL translation exists for 'sort_T_m__Function_$0_1$__...'` |
| `zip` | 9 | `No SQL translation exists for 'zip_T_MANY__U_MANY__Pair_MANY_'` |
| `format` | 14 | `No SQL translation exists for 'format_String_1__Any_MANY__String_1_'` |
| `split` | 2 | `No SQL translation exists for 'split_String_1__String_1__String_MANY_'` |
| `compare` | 3+ | `No SQL translation exists for 'compare_T_1__T_1__Integer_1_'` |
| `removeDuplicates` | 5 | `No SQL translation exists for 'removeDuplicates_T_MANY__...'` |
| `date` construction | 3 | `No SQL translation exists for 'date_Integer_1__...'` |
| `has*` (date functions) | 9 | `No SQL translation exists for 'hasDay/hasMonth/etc'` |
| `match` (with params) | 2 | `No SQL translation exists for 'match_Any_MANY__Function_$1_MANY$__...'` |
| `indexOf` (from index) | 1 | `No SQL translation exists for 'indexOf_String_1__String_1__Integer_1__Integer_1_'` |
| `letFunction` | 4 | `No SQL translation exists for 'letFunction_String_1__T_m__T_m_'` |
| `add` (with offset) | 1 | `No SQL translation exists for 'add_T_MANY__Integer_1__T_1__...'` |
| `parseBoolean` | 2 | `[unsupported-api] The function 'parseBoolean' is not supported yet` |
| `toTimestamp/parseDate` | 4 | `[unsupported-api] The function 'toTimestamp' is not supported yet` |

### 2. DuckDB Limitations (unsupportedFeature)

| Feature | Count | Error Pattern |
|---------|-------|---------------|
| YEAR and YEAR-MONTH support | 8+ | `DuckDB doesn't support YEAR and YEAR-MONTH` |
| Date has no day | 4 | `Date has no day: YYYY-MM` |
| Non-Primitive filter expressions | 5 | `Filter expressions are only supported for Primitives and Enums` |
| Base64 decode without padding | 1 | `Could not decode string as base64: length must be a multiple of 4` |

### 3. Data Type/Format Issues (needsInvestigation)

| Issue | Count | Error Pattern |
|-------|-------|---------------|
| DateTime format mismatch | 10+ | DateTime format differences (e.g., nanoseconds vs milliseconds) |
| Decimal/Float precision | 15+ | Expected vs actual decimal representation differences |
| Integer overflow | 5+ | `Overflow in multiplication/addition/subtraction of INT64` |
| Type casting issues | 5+ | `Conversion Error: Unimplemented type for cast` |

### 4. Multiplicity/Collection Issues (needsInvestigation)

| Issue | Count | Error Pattern |
|-------|-------|---------------|
| Multiplicity cast issues | 10+ | `Cannot cast a collection of size X to multiplicity [1]` |
| Empty collection access | 5+ | `The system is trying to get an element at offset X where the collection is of size 0` |
| Expected at most one | 1 | `Expected at most one object, but found many` |

### 5. Match Function Issues (needsInvestigation)

| Issue | Count | Error Pattern |
|-------|-------|---------------|
| Match non-primitive return | 8 | `Match does not support Non-Primitive return type` |
| Match multiplicity issues | 2 | `Match only supports operands with multiplicity [1]` |
| Cast exceptions in Match | 2 | `Cast exception: Literal cannot be cast to SemiStructuredPropertyAccess` |

### 6. Lambda/Function Processing Issues (needsInvestigation)

| Issue | Count | Error Pattern |
|-------|-------|---------------|
| Fold type mismatch | 6 | `The initial value type must be the same as the list child type` |
| Copy function missing | 3 | `Function does not exist 'copy(...)'` |
| Find function missing | 2 | `Function does not exist 'find(...)'` |

### 7. Index/Offset Differences (needsInvestigation)

| Issue | Count | Error Pattern |
|-------|-------|---------------|
| 0-indexed vs 1-indexed | 4+ | Expected index differs by 1 (Pure 0-indexed vs SQL 1-indexed) |

### 8. At Function Limitations (needsImplementation)

| Issue | Count | Error Pattern |
|-------|-------|---------------|
| At after properties only | 3 | `->at(...) function is supported only after direct access of 1->MANY properties` |

---

## Detailed Failure List by Test Suite

### EssentialFunctions PCT (111 failures)

| Category | Test Name | Error | Qualifier |
|----------|-----------|-------|-----------|
| **Add** | testAddWithOffset | No SQL translation for 'add_T_MANY__Integer_1__T_1__T_$1_MANY$_' | needsImplementation |
| **Concatenate** | testConcatenateMixedType | Any is not managed yet! | needsInvestigation |
| **Concatenate** | testConcatenateTypeInference | Any is not managed yet! | needsInvestigation |
| **Contains** | testContainsNonPrimitive | Parameter to IN operation isn't a literal! | unsupportedFeature |
| **Contains** | testContainsPrimitive | Conversion Error: Unimplemented type for cast (INTEGER -> DATE) | needsInvestigation |
| **Contains** | testContainsWithFunction | no viable alternative at input | needsInvestigation |
| **Exists** | testExists | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **Find** | testFindInstance | Error dynamically evaluating value specification | needsInvestigation |
| **Find** | testFindLiteralFromVar | Function does not exist 'find(String[3],...)' | needsImplementation |
| **Find** | testFindLiteral | Function does not exist 'find(String[4],...)' | needsImplementation |
| **Find** | testFindUsingVarForFunction | Error dynamically evaluating value specification | needsInvestigation |
| **Fold** | testFoldCollectionAccumulator | Initial value type must be same as list child type | needsInvestigation |
| **Fold** | testFoldWithEmptyAccumulator | Initial value type must be same as list child type | needsInvestigation |
| **Fold** | testFoldWithSingleValue | Initial value type must be same as list child type | needsInvestigation |
| **Fold** | testFoldEmptyListAndEmptyIdentity | Any is not managed yet! | needsInvestigation |
| **Fold** | testFoldFiltering | Function does not exist 'copy(...)' | unsupportedFeature |
| **Fold** | testFoldMixedAccumulatorTypes | Initial value type must be same as list child type | needsInvestigation |
| **Fold** | testFoldToMany | Function does not exist 'copy(...)' | unsupportedFeature |
| **Fold** | testFold | Function does not exist 'copy(...)' | unsupportedFeature |
| **ForAll** | testforAllOnEmptySet | No SQL translation for 'forAll_T_MANY__Function_1__Boolean_1_' | needsImplementation |
| **ForAll** | testforAllOnNonEmptySetIsFalse | No SQL translation for 'forAll_T_MANY__Function_1__Boolean_1_' | needsImplementation |
| **ForAll** | testforAllOnNonEmptySetIsTrue | No SQL translation for 'forAll_T_MANY__Function_1__Boolean_1_' | needsImplementation |
| **Head** | testHeadComplex | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **Head** | testHeadOnEmptySet | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **Head** | testHeadOnOneElement | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **Head** | testHeadSimple | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **IndexOf** | testIndexOfOneElement | expected: 0, actual: 1 | needsInvestigation |
| **Mod** | testModInEval | Unused format args | needsInvestigation |
| **Pow** | testNumberPow | expected: 9.0, actual: 27.0 | needsInvestigation |
| **Sort** | testMixedSortNoComparator | Not supported: Number | needsInvestigation |
| **Sort** | testSimpleSortReversed | No SQL translation for 'sort_T_m__Function_$0_1$__...' | needsImplementation |
| **Sort** | testSimpleSortWithFunctionVariables | No SQL translation for 'sort_T_m__Function_$0_1$__...' | needsImplementation |
| **Sort** | testSimpleSortWithKey | No SQL translation for 'sort_T_m__Function_$0_1$__...' | needsImplementation |
| **Sort** | testSimpleSort | No SQL translation for 'sort_T_m__Function_$0_1$__...' | needsImplementation |
| **Zip** | testZipBothListsAreOfPairs | No SQL translation for 'zip_T_MANY__U_MANY__Pair_MANY_' | needsImplementation |
| **Zip** | testZipBothListsEmpty | Trying to get element at offset 0 where collection size is 0 | needsInvestigation |
| **Zip** | testZipBothListsSameLength | No SQL translation for 'zip_T_MANY__U_MANY__Pair_MANY_' | needsImplementation |
| **Zip** | testZipFirstListEmpty | Trying to get element at offset 0 where collection size is 0 | needsInvestigation |
| **Zip** | testZipFirstListLonger | No SQL translation for 'zip_T_MANY__U_MANY__Pair_MANY_' | needsImplementation |
| **Zip** | testZipFirstListsIsOfPairs | No SQL translation for 'zip_T_MANY__U_MANY__Pair_MANY_' | needsImplementation |
| **Zip** | testZipSecondListEmpty | Trying to get element at offset 0 where collection size is 0 | needsInvestigation |
| **Zip** | testZipSecondListLonger | No SQL translation for 'zip_T_MANY__U_MANY__Pair_MANY_' | needsImplementation |
| **Zip** | testZipSecondListsIsOfPairs | No SQL translation for 'zip_T_MANY__U_MANY__Pair_MANY_' | needsImplementation |
| **Format** | testFormatBoolean | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatDate | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatFloatWithRounding | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatFloatWithTruncation | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatFloatWithZeroPadding | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatFloat | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatInEval | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatInEvaluate | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **Format** | testFormatIntegerWithZeroPadding | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatInteger | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatList | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **Format** | testFormatPair | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **Format** | testFormatRepr | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testFormatString | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **Format** | testSimpleFormatDate | No SQL translation for 'format_String_1__Any_MANY__String_1_' | unsupportedFeature |
| **JoinStrings** | testJoinStringsNoStrings | expected: '', actual: '[]' | needsInvestigation |
| **JoinStrings** | testJoinStringsSingleString | expected: '[a]', actual: '[' | needsInvestigation |
| **JoinStrings** | testJoinStringsUsingGenericArrow | expected: '[a,b,c]', actual: '[,a,b,c,]' | needsInvestigation |
| **JoinStrings** | testJoinStrings | expected: '[a,b,c]', actual: '[,a,b,c,]' | needsInvestigation |
| **Split** | testSplitWithNoSplit | No SQL translation for 'split_String_1__String_1__String_MANY_' | needsImplementation |
| **Split** | testSplit | No SQL translation for 'split_String_1__String_1__String_MANY_' | needsImplementation |
| **Substring** | testStartEnd | expected: 'the quick...', actual: 'the quick...' (off by one) | needsInvestigation |
| **Substring** | testStart | expected: 'he quick...', actual: 'the quick...' | needsInvestigation |
| **ToString** | testClassToString | Match failure: ClassInstanceHolderObject | needsInvestigation |
| **ToString** | testComplexClassToString | type not supported: ErrorType | needsInvestigation |
| **ToString** | testDateTimeToString | DateTime format mismatch | needsInvestigation |
| **ToString** | testDateTimeWithTimezoneToString | DateTime format mismatch | needsInvestigation |
| **ToString** | testDateToString | Date has no day: 2014-01 | unsupportedFeature |
| **ToString** | testEnumerationToString | Match failure: ClassInstanceHolderObject | needsInvestigation |
| **ToString** | testListToString | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **ToString** | testPairCollectionToString | Cannot cast collection of size 2 to multiplicity [1] | needsInvestigation |
| **ToString** | testPairToString | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **ToString** | testPersonToString | Assert failed | needsInvestigation |
| **ToString** | testSimpleDateToString | DateTime format mismatch | needsInvestigation |
| **Rem** | testRemWithDecimals | expected: 0.14D, actual: 0.14 | needsInvestigation |
| **Rem** | testRemInEvalWithNegativeNumbers | Unused format args | needsInvestigation |
| **Rem** | testRemInEval | Unused format args | needsInvestigation |
| **Round** | testPositiveFloatRoundHalfEvenDown | expected: 16, actual: 17 | needsInvestigation |
| **Round** | testNegativeFloatRoundHalfEvenUp | expected: -16, actual: -17 | needsInvestigation |
| **ToDecimal** | testIntToDecimal | expected: 8D, actual: 8.0D | needsInvestigation |
| **At** | testAtOtherScenario | ->at(...) only supported after direct 1->MANY properties | needsInvestigation |
| **At** | testAtWithVariable | ->at(...) only supported after direct 1->MANY properties | needsInvestigation |
| **At** | testAt | ->at(...) only supported after direct 1->MANY properties | needsInvestigation |
| **RemoveDuplicates** | testRemoveDuplicatesEmptyListExplicit | No SQL translation for 'removeDuplicates_T_MANY__...' | needsImplementation |
| **RemoveDuplicates** | testRemoveDuplicatesPrimitiveNonStandardFunction | No SQL translation for 'removeDuplicates_T_MANY__...' | needsImplementation |
| **RemoveDuplicates** | testRemoveDuplicatesPrimitiveStandardFunctionExplicit | No SQL translation for 'removeDuplicates_T_MANY__...' | needsImplementation |
| **RemoveDuplicates** | testRemoveDuplicatesPrimitiveStandardFunctionMixedTypes | Any is not managed yet! | needsInvestigation |
| **RemoveDuplicatesBy** | testRemoveDuplicatesByPrimitive | No SQL translation for 'removeDuplicates_T_MANY__...' | needsImplementation |
| **Date** | testAdjustByDaysBigNumber | INT64 value out of range for INT32 | needsInvestigation |
| **Date** | testAdjustByHoursBigNumber | Interval value out of range | needsInvestigation |
| **Date** | testAdjustByMicrosecondsBigNumber | DateTime format precision mismatch | needsInvestigation |
| **Date** | testAdjustByMinutesBigNumber | Date calculation overflow | needsInvestigation |
| **Date** | testAdjustByMonthsBigNumber | INT64 value out of range for INT32 | needsInvestigation |
| **Date** | testAdjustReflectiveEvaluation | Can't find match for 'eval' function | needsImplementation |
| **Date** | testDateFromHour | No SQL translation for 'date_Integer_1__...' | needsImplementation |
| **Date** | testDateFromMinute | No SQL translation for 'date_Integer_1__...' | needsImplementation |
| **Date** | testDateFromMonth | No SQL translation for 'date_Integer_1__...' | needsImplementation |
| **Date** | testDateFromSecond | DateTime format precision mismatch | needsInvestigation |
| **Date** | testDateFromSubSecond | DateTime format precision mismatch | needsInvestigation |
| **Date** | testDateFromYear | No SQL translation for 'date_Integer_1__Date_1_' | unsupportedFeature |
| **Date** | testHasDay | No SQL translation for 'hasDay_Date_1__Boolean_1_' | unsupportedFeature |
| **Date** | testHasHour | No SQL translation for 'hasHour_Date_1__Boolean_1_' | unsupportedFeature |
| **Date** | testHasMinute | No SQL translation for 'hasMinute_Date_1__Boolean_1_' | unsupportedFeature |
| **Date** | testHasMonthReflect | No SQL translation for 'hasMonth_Date_1__Boolean_1_' | unsupportedFeature |
| **Date** | testHasMonth | No SQL translation for 'hasMonth_Date_1__Boolean_1_' | unsupportedFeature |
| **Date** | testHasSecond | No SQL translation for 'hasSecond_Date_1__Boolean_1_' | unsupportedFeature |
| **Date** | testHasSubsecondWithAtLeastPrecision | No SQL translation for 'hasSubsecondWithAtLeastPrecision_...' | unsupportedFeature |
| **Date** | testHasSubsecond | No SQL translation for 'hasSubsecond_Date_1__Boolean_1_' | unsupportedFeature |
| **Date** | testAdjustByMonths | Date has no day: 2012-03 | unsupportedFeature |
| **Date** | testAdjustByWeeksBigNumber | INT64 value out of range for INT32 | needsInvestigation |
| **Date** | testAdjustByYears | DuckDB doesn't support YEAR and YEAR-MONTH | unsupportedFeature |
| **Date** | testDateDiffWeeks | expected: 1, actual: 0 | needsInvestigation |
| **Date** | testDateDiffYears | DuckDB doesn't support YEAR and YEAR-MONTH | unsupportedFeature |
| **Date** | testDatePartYearMonthOnly | Date has no day: 1973-11 | unsupportedFeature |
| **Date** | testDatePartYearOnly | DuckDB doesn't support YEAR and YEAR-MONTH | unsupportedFeature |
| **Date** | testHour | expected: 17, actual: 0 | needsInvestigation |
| **Date** | testMinute | expected: 9, actual: 0 | needsInvestigation |
| **Date** | testMonthNumber | Date has no day: 2015-04 | unsupportedFeature |
| **Date** | testYear | DuckDB doesn't support YEAR and YEAR-MONTH | unsupportedFeature |
| **Match** | testMatchManyWithMany | Match only supports operands with multiplicity [1] | needsInvestigation |
| **Match** | testMatchOneWithMany | Match does not support Non-Primitive return type | needsInvestigation |
| **Match** | testMatchOneWithZeroOne | Match does not support Non-Primitive return type | needsInvestigation |
| **Match** | testMatchOneWith | Match does not support Non-Primitive return type | needsInvestigation |
| **Match** | testMatchWithExtraParam | No SQL translation for 'match_Any_MANY__Function_$1_MANY$__...' | needsImplementation |
| **Match** | testMatchWithExtraParamsAndFunctionsAsParam | No SQL translation for 'match_Any_MANY__Function_$1_MANY$__...' | needsImplementation |
| **Match** | testMatchWithFunctionsAsParamManyMatch | Match does not support Non-Primitive return type | needsInvestigation |
| **Match** | testMatchWithFunctionsAsParam | Cast exception: Literal cannot be cast to SemiStructuredPropertyAccess | needsInvestigation |
| **Match** | testMatchWithFunctionsManyMatch | Match does not support Non-Primitive return type | needsInvestigation |
| **Match** | testMatchWithFunctions | Cast exception: Literal cannot be cast to SemiStructuredPropertyAccess | needsInvestigation |
| **Match** | testMatchWithMixedReturnType | type not supported: GeographicEntityType | needsInvestigation |
| **Match** | testMatchZeroWithMany | Match does not support Non-Primitive return type | needsInvestigation |
| **Match** | testMatchZeroWithZero | Match does not support Non-Primitive return type | needsInvestigation |
| **Match** | testMatch | type not supported: GeographicEntityType | needsInvestigation |
| **IndexOf** | testFromIndex | No SQL translation for 'indexOf_String_1__String_1__Integer_1__Integer_1_' | needsImplementation |
| **IndexOf** | testSimple | expected: 4, actual: 5 | needsInvestigation |
| **ParseBoolean** | testParseFalse | [unsupported-api] 'parseBoolean' not supported | needsImplementation |
| **ParseBoolean** | testParseTrue | [unsupported-api] 'parseBoolean' not supported | needsImplementation |
| **ParseDate** | testParseDateTypes | [unsupported-api] 'toTimestamp' not supported | needsImplementation |
| **ParseDate** | testParseDateWithTimezone | [unsupported-api] 'toTimestamp' not supported | needsImplementation |
| **ParseDate** | testParseDateWithZ | [unsupported-api] 'toTimestamp' not supported | needsImplementation |
| **ParseDate** | testParseDate | [unsupported-api] 'toTimestamp' not supported | needsImplementation |
| **ParseDecimal** | testParseDecimal | Could not convert string "3.14159d" to DECIMAL | needsInvestigation |
| **ParseDecimal** | testParseDecimalWithPrecisionScale | Decimal precision mismatch | needsInvestigation |
| **ParseDecimal** | testParseZero | Decimal precision mismatch | needsInvestigation |

### RelationFunctions PCT (1 failure)

| Category | Test Name | Error | Qualifier |
|----------|-----------|-------|-----------|
| **Over/Range** | testRange_WithNumbers_CurrentRow_NFollowing_WithoutPartition_WithSingleOrderBy | Floating point precision mismatch in TDS output | needsInvestigation |

### StandardFunctions PCT (35 failures)

| Category | Test Name | Error | Qualifier |
|----------|-----------|-------|-----------|
| **In** | testInIsEmpty | NullPointer exception | needsInvestigation |
| **In** | testInNonPrimitive | Parameter to IN operation isn't a literal! | unsupportedFeature |
| **In** | testInPrimitive | Conversion Error: Unimplemented type for cast | needsInvestigation |
| **Covariance/Correlation** | testCorr | Unused format args | needsInvestigation |
| **Covariance/Correlation** | testCovarPopulation | Unused format args | needsInvestigation |
| **Covariance/Correlation** | testCovarSample | Unused format args | needsInvestigation |
| **Numeric** | testToDegrees | Overflow in multiplication of DECIMAL(18) | needsInvestigation |
| **Numeric** | testToRadians | Overflow in multiplication of DECIMAL(18) | needsInvestigation |
| **Max** | testMax_Numbers | expected: 2, actual: 2.0 | needsInvestigation |
| **Max** | testMax | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **MaxBy** | testMaxBy | No function matches 'max_by(INTEGER, INTEGER, INTEGER, INTEGER)' | needsInvestigation |
| **Min** | testMin_Numbers | expected: 1.23D, actual: 1.23 | needsInvestigation |
| **Min** | testMin | Cannot cast collection of size 0 to multiplicity [1] | needsInvestigation |
| **MinBy** | testMinBy | No function matches 'min_by(INTEGER, INTEGER, INTEGER, INTEGER)' | needsInvestigation |
| **Median** | testMedian_Floats | Unused format args | needsInvestigation |
| **Median** | testMedian_Integers | Unused format args | needsInvestigation |
| **Median** | testMedian_Numbers | Unused format args | needsInvestigation |
| **Median** | testMedian_Numbers_Relation_Window | Floating point precision mismatch | needsInvestigation |
| **Mode** | testMode_Float | Unused format args | needsInvestigation |
| **Mode** | testMode_Integer | Unused format args | needsInvestigation |
| **Mode** | testMode_Number | Unused format args | needsInvestigation |
| **Average** | testAverage_Floats | Unused format args | needsInvestigation |
| **Average** | testAverage_Integers | Unused format args | needsInvestigation |
| **Average** | testAverage_Numbers | Unused format args | needsInvestigation |
| **Percentile** | testPercentile | No function matches 'quantile_cont(BIGINT[], DECIMAL)' | needsInvestigation |
| **Percentile** | testPercentile_Relation_Window | percentile_cont does not exist | needsInvestigation |
| **CosH** | testCosH_EvalFuncSig | Unused format args | needsInvestigation |
| **SinH** | testSinH_EvalFuncSig | Unused format args | needsInvestigation |
| **TanH** | testTanH_EvalFuncSig | Unused format args | needsInvestigation |
| **Bitwise** | testBitShiftRight_MoreThan62Bits | Error message mismatch - No error was thrown | assertErrorMismatch |
| **Bitwise** | testBitShiftLeft_MoreThan62Bits | Error message mismatch | assertErrorMismatch |
| **Hash** | testHashCodeAggregate | [unsupported-api] 'hashAgg' not supported | needsInvestigation |
| **And** | testAnd | Can't find packageable element 'andtrue' | needsInvestigation |
| **Or** | testOr | Can't find packageable element 'ortrue' | needsInvestigation |
| **Greatest** | testGreatest_DateTime | DateTime format mismatch (nanoseconds) | needsInvestigation |
| **Greatest** | testGreatest_Number | expected: 2, actual: 2.0 | needsInvestigation |
| **Greatest** | testGreatest_Single | DateTime format mismatch (nanoseconds) | needsInvestigation |
| **Least** | testLeast_DateTime | DateTime format mismatch (nanoseconds) | needsInvestigation |
| **Least** | testLeast_Number | expected: 1.0D, actual: 1.0 | needsInvestigation |
| **Least** | testLeast_Single | DateTime format mismatch (nanoseconds) | needsInvestigation |

### GrammarFunctions PCT (32 failures)

| Category | Test Name | Error | Qualifier |
|----------|-----------|-------|-----------|
| **Not** | testNotInCollection | ->at(...) function is supported only after direct access | needsImplementation |
| **Eq** | testEqDate | DuckDB doesn't support YEAR and YEAR-MONTH | unsupportedFeature |
| **Eq** | testEqNonPrimitive | Filter expressions only supported for Primitives and Enums | unsupportedFeature |
| **Eq** | testEqVarIdentity | Filter expressions only supported for Primitives and Enums | unsupportedFeature |
| **Eq** | testEqEnum | Assert failed | needsInvestigation |
| **Eq** | testEqPrimitiveExtension | Filter expressions only supported for Primitives and Enums | unsupportedFeature |
| **Equal** | testEqualEnum | Assert failed | needsInvestigation |
| **Equal** | testEqualNonPrimitive | Filter expressions only supported for Primitives and Enums | unsupportedFeature |
| **Equal** | testEqualVarIdentity | Filter expressions only supported for Primitives and Enums | unsupportedFeature |
| **Equal** | testEqualDateStrictYear | DuckDB doesn't support YEAR and YEAR-MONTH | unsupportedFeature |
| **Equal** | testEqualPrimitiveExtension | Filter expressions only supported for Primitives and Enums | unsupportedFeature |
| **GreaterThan** | testGreaterThan_Boolean | Assert failed | needsInvestigation |
| **GreaterThanEqual** | testGreaterThanEqual_Boolean | Assert failed | needsInvestigation |
| **Filter** | testFilterInstance | Error dynamically evaluating value specification | unsupportedFeature |
| **First** | testFirstComplex | Expected at most one object, but found many | needsImplementation |
| **Map** | testMapInstance | type not supported: GeographicEntityType | needsInvestigation |
| **Map** | testMapRelationshipFromManyToMany | Error dynamically evaluating value specification | unsupportedFeature |
| **Map** | testMapRelationshipFromManyToOne | Error dynamically evaluating value specification | unsupportedFeature |
| **Map** | testMapRelationshipFromOneToOne | Error dynamically evaluating value specification | unsupportedFeature |
| **Compare** | (all tests in package) | No SQL translation for 'compare_T_1__T_1__Integer_1_' | unsupportedFeature |
| **Minus** | testDecimalMinus | expected: -4.0D, actual: -4.0 | needsInvestigation |
| **Minus** | testLargeMinus | Overflow in subtraction of INT64 | needsInvestigation |
| **Minus** | testSingleMinusType | No SQL translation for 'genericType_Any_MANY__GenericType_1_' | unsupportedFeature |
| **Minus** | testSingleMinus | SELECT clause without selection list | needsInvestigation |
| **Plus** | testDecimalPlus | expected: 6.0D, actual: 6.0 | needsInvestigation |
| **Plus** | testLargePlus | Overflow in addition of INT64 | needsInvestigation |
| **Plus** | testSinglePlusType | No SQL translation for 'genericType_Any_MANY__GenericType_1_' | unsupportedFeature |
| **Times** | testDecimalTimes | expected: 353791.470D, actual: 353791.47 | needsInvestigation |
| **Times** | testLargeTimes | Overflow in multiplication of INT64 | needsInvestigation |
| **String Plus** | testMultiPlusWithPropertyExpressions | type not supported: GeographicEntityType | needsInvestigation |
| **String Plus** | testPlusInCollect | No SQL translation for 'at_T_MANY__Integer_1__T_1_' | needsImplementation |
| **String Plus** | testPlusInIterate | Match failure: StoreMappingClusteredValueSpecificationObject | needsInvestigation |
| **Let** | testAssignNewInstance | type not supported: GeographicEntityType | needsInvestigation |
| **Let** | testLetAsLastStatement | No SQL translation for 'letFunction_String_1__T_m__T_m_' | needsInvestigation |
| **Let** | testLetChainedWithAnotherFunction | No SQL translation for 'letFunction_String_1__T_m__T_m_' | needsInvestigation |
| **Let** | testLetInsideIf | No SQL translation for 'letFunction_String_1__T_m__T_m_' | needsInvestigation |
| **Let** | testLetWithParam | No SQL translation for 'letFunction_String_1__T_m__T_m_' | needsInvestigation |

### UnclassifiedFunctions PCT (12 failures)

| Category | Test Name | Error | Qualifier |
|----------|-----------|-------|-----------|
| **SplitPart** | testSplitPartEmptyToken | expected: 'Hello World', actual: 'H' | needsInvestigation |
| **Base64** | testDecodeBase64NoPadding | Could not decode string as base64: length must be multiple of 4 | unsupportedFeature |
| **Lpad** | testLpadEmptyChar | Insufficient padding in LPAD | needsInvestigation |
| **Rpad** | testRpadEmptyChar | Insufficient padding in RPAD | needsInvestigation |
| **ToLowerFirstCharacter** | TestToLowerFirstCharacter | expected: 'xoXoXoX', actual: 'XoXoXoX' | needsInvestigation |
| **ToUpperFirstCharacter** | TestToUpperFirstCharacter | expected: 'XOxOxOx', actual: 'xOxOxOx' | needsInvestigation |
| **RegexpIndexOf** | testRegexpIndexOf | expected: 3, actual: 4 (0 vs 1-indexed) | needsInvestigation |
| **RegexpIndexOf** | testRegexpIndexOf_GroupNumber | expected: 3, actual: 4 (0 vs 1-indexed) | needsInvestigation |
| **RegexpLike** | testRegexpLike_CaseInsensitive_Multiline | Assert failed (multiline regexp param) | needsInvestigation |
| **RegexpLike** | testRegexpLike_CaseInsensitive_Multiline_NonNewlineSensitive | Assert failed (multiline regexp param) | needsInvestigation |
| **RegexpLike** | testRegexpLike_Multiline | Assert failed (multiline regexp param) | needsInvestigation |
| **RegexpLike** | testRegexpLike_Multiline_NonNewlineSensitive | Assert failed (multiline regexp param) | needsInvestigation |

### VariantFunctions PCT (16 failures)

| Category | Test Name | Error | Qualifier |
|----------|-----------|-------|-----------|
| **To** | testToBooleanFromBadString | Error message mismatch: expected "Invalid Pure Boolean: 'hello'" | assertErrorMismatch |
| **To** | testToDateTimeFromWrongString | No error thrown, expected "DateTime must include time information" | needsInvestigation |
| **To** | testToIntegerFromFloat | No error thrown, expected "Integer is not managed yet!" | needsInvestigation |
| **To** | testToIntegerFromStringFloat | Error message mismatch | assertErrorMismatch |
| **To** | testToStrictDateFromWrongString | Error message mismatch | assertErrorMismatch |
| **To** | testToListFromNonArrayVariant | Error message mismatch | assertErrorMismatch |
| **To** | testToListOfVariants | Integer is not managed yet! | needsImplementation |
| **To** | testToDateTime | DateTime format mismatch (nanoseconds) | needsInvestigation |
| **ToVariant** | testListOfList | expected: '[[[1]]]', actual: '[1]' | needsInvestigation |
| **ToVariant** | testListOfMap | Unimplemented type for cast (MAP -> MAP[]) | needsInvestigation |
| **ToMany** | testToManyFromNonArray | Error message mismatch | assertErrorMismatch |
| **ToMany** | testToManyVariant | Integer is not managed yet! | needsInvestigation |
| **ToClass** | testToClassWithInheritance | mapping missing and cannot construct return type for class: Pet | needsInvestigation |
| **ToClass** | testToClass | mapping missing and cannot construct return type for class: Person | needsInvestigation |
| **ToClass** | testToClassWithInheritance (toMany) | mapping missing and cannot construct return type for class: Pet | needsInvestigation |
| **ToClass** | testToClass (toMany) | mapping missing and cannot construct return type for class: Person | needsInvestigation |

### ScenarioQuantFunctions PCT (0 failures)

No failures in this test suite.

---

## Summary by Qualifier

| Qualifier | Count | Description |
|-----------|-------|-------------|
| **needsImplementation** | ~50 | SQL translation or API support needs to be added |
| **needsInvestigation** | ~120 | Requires investigation to determine root cause |
| **unsupportedFeature** | ~30 | Feature not supported by DuckDB |
| **assertErrorMismatch** | ~7 | Error message assertion mismatch |

---

## Recommended Priority Actions

### High Priority (Quick Wins)
1. **Fix index offset issues** (~5+ tests) - Align 0-indexed (Pure) vs 1-indexed (SQL) behavior
2. **Fix DateTime format output** (~15+ tests) - Standardize nanoseconds vs milliseconds format
3. **Fix Decimal precision representation** (~10+ tests) - Ensure consistent D suffix handling

### Medium Priority (Feature Gaps)
4. **Implement zip function** (~9 tests) - Collection operation
5. **Implement format function** (~14 tests) - String formatting support
6. **Implement forAll function** (~3 tests) - Collection predicate
7. **Implement sort with comparator** (~4 tests) - Advanced sorting
8. **Implement removeDuplicates** (~5 tests) - Collection operation
9. **Implement date construction functions** (~3 tests) - Date creation from parts
10. **Implement has* date functions** (~9 tests) - Date introspection

### Lower Priority (DB Limitations)
11. **Document DuckDB limitations** - YEAR/YEAR-MONTH types, base64 padding
12. **Implement workarounds** for Match function limitations, overflow handling