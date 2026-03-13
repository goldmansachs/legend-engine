// Copyright 2023 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package org.finos.legend.engine.query.sql.api;

import org.finos.legend.engine.protocol.sql.metamodel.AllColumns;
import org.finos.legend.engine.protocol.sql.metamodel.Query;
import org.finos.legend.engine.protocol.sql.metamodel.QueryBody;
import org.finos.legend.engine.protocol.sql.metamodel.QuerySpecification;
import org.finos.legend.engine.protocol.sql.metamodel.SelectItem;
import org.finos.legend.engine.protocol.sql.metamodel.TableSubquery;

/**
 * Normalizes SQL queries to a canonical form for caching purposes.
 * This allows semantically equivalent queries to share the same cache entry.
 *
 * Normalizations performed:
 * - Unwrap unnecessary subquery wrappers: SELECT * FROM (SELECT ... FROM X) → SELECT ... FROM X
 */
public class QueryNormalizer
{
    private QueryNormalizer()
    {
    }

    public static Query normalize(Query query)
    {
        if (query == null || query.queryBody == null)
        {
            return query;
        }

        QueryBody normalizedBody = normalizeQueryBody(query.queryBody);

        if (normalizedBody == query.queryBody)
        {
            return query;
        }

        Query normalized = new Query();
        normalized.queryBody = normalizedBody;
        return normalized;
    }

    private static QueryBody normalizeQueryBody(QueryBody body)
    {
        if (body instanceof QuerySpecification)
        {
            return normalizeQuerySpecification((QuerySpecification) body);
        }
        return body;
    }

    /**
     * Normalizes a QuerySpecification by unwrapping unnecessary subquery wrappers.
     *
     * Pattern: SELECT * FROM (subquery) with no additional clauses
     * Result: The inner query body
     */
    private static QueryBody normalizeQuerySpecification(QuerySpecification spec)
    {
        if (!isPassThroughWrapper(spec))
        {
            return spec;
        }

        TableSubquery subquery = (TableSubquery) spec.from.get(0);
        Query innerQuery = subquery.query;

        if (innerQuery == null || innerQuery.queryBody == null)
        {
            return spec;
        }

        // Recursively normalize the inner query body
        return normalizeQueryBody(innerQuery.queryBody);
    }

    /**
     * Checks if a QuerySpecification is a pass-through wrapper (SELECT * FROM (subquery) with no other clauses).
     */
    private static boolean isPassThroughWrapper(QuerySpecification spec)
    {
        // Must have SELECT *
        if (!isSelectAll(spec))
        {
            return false;
        }

        // Must have exactly one FROM item that is a TableSubquery
        if (spec.from == null || spec.from.size() != 1 || !(spec.from.get(0) instanceof TableSubquery))
        {
            return false;
        }

        // Must not have any additional clauses
        return spec.where == null
                && (spec.groupBy == null || spec.groupBy.isEmpty())
                && spec.having == null
                && (spec.orderBy == null || spec.orderBy.isEmpty())
                && spec.limit == null
                && spec.offset == null;
    }

    /**
     * Checks if the SELECT clause is "SELECT *" (all columns, no prefix).
     */
    private static boolean isSelectAll(QuerySpecification spec)
    {
        if (spec.select == null || spec.select.selectItems == null || spec.select.selectItems.size() != 1)
        {
            return false;
        }

        SelectItem item = spec.select.selectItems.get(0);

        if (!(item instanceof AllColumns))
        {
            return false;
        }

        AllColumns allColumns = (AllColumns) item;
        return allColumns.prefix == null || allColumns.prefix.isEmpty();
    }
}

