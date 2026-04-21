# Data Profiling

## Overview

The **data profiling** feature produces aggregate statistics for every column in a relation.
For each column it computes metrics such as total count, distinct count, null count, and
min/max values (for numeric and date types).

The caller supplies a relation in one of three ways:

* a **`packagePath`** pointing to a `DataQualityRelationValidation` — the query is extracted
  from the validation element automatically;
* a **`packagePath`** pointing to a `ConcreteFunctionDefinition` that returns a `Relation` —
  the function's expression is wrapped and used directly;
* an **inline `LambdaFunction`** (the `query` field) — the lambda must return a `Relation` and
  end with `->from(…)`.

---

## Output Shape

The result is a single TDS with a fixed set of columns.  There is **one row per column** in
the source relation.

| Column | Type | Description |
|--------|------|-------------|
| `column_name` | `String` | Name of the source relation column |
| `count` | `Integer` | Total number of rows |
| `count_distinct` | `Integer` | Number of distinct non-null values |
| `count_null` | `Integer` | Number of null / missing values |
| `number_max` | `Number` | Maximum value (numeric columns only, null otherwise) |
| `number_min` | `Number` | Minimum value (numeric columns only, null otherwise) |
| `date_max` | `Date` | Maximum value (date columns only, null otherwise) |
| `date_min` | `Date` | Minimum value (date columns only, null otherwise) |

**Example:**

```
#TDS
column_name, count, count_distinct, count_null, number_max, number_min, date_max, date_min
id,          6,     6,              0,          5,          -1,         null,     null
fullName,    6,     6,              0,          null,       null,       null,     null
age,         6,     4,              1,          35,         25,         null,     null
annualSalary,6,     5,              0,          75000.75,   30000.00,   null,     null
dateOfBirth, 6,     3,              2,          null,       null,       2000-03-01, 1990-01-01
#
```

### Key design points

* Rows are ordered by **relation column order**.
* The result is produced as a single query built as a **UNION ALL** of per-column aggregate
  sub-queries.
* When using a `DataQualityRelationValidation`, the `from()` wrapping (mapping + runtime) is
  extracted from the validation's query automatically.
* When using a `ConcreteFunctionDefinition`, the function must return a `Relation` ending with
  `->from(…)`.

---

## REST API

### Profile

`POST /pure/v1/dataquality/profile` — executes the profiling query and returns a TDS result.

#### Request body — `DataQualityProfileInput`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `clientVersion` | `String` | yes | Protocol version (e.g. `vX_X_X`) |
| `model` | `PureModelContext` | yes | Model context (pointer or data) |
| `packagePath` | `String` | one of `packagePath` / `query` | Path to a `DataQualityRelationValidation` or a `ConcreteFunctionDefinition` that returns a `Relation` |
| `query` | `LambdaFunction` | one of `packagePath` / `query` | Inline relation query ending with `->from(…)` |
| `lambdaParameterValues` | `List<ParameterValue>` | no | Parameter bindings for the query |

> `packagePath` and `query` are **mutually exclusive** — supply exactly one.

---

## See Also

* [Data Quality Overview](data-quality-overview.md)
* [Sample Values](sample-values.md)
* Source: `core_dataquality/generation/dataprofile.pure`

