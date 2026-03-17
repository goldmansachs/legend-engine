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

import org.finos.legend.engine.protocol.sql.metamodel.AliasedRelation;
import org.finos.legend.engine.protocol.sql.metamodel.AllColumns;
import org.finos.legend.engine.protocol.sql.metamodel.Query;
import org.finos.legend.engine.protocol.sql.metamodel.QuerySpecification;
import org.finos.legend.engine.protocol.sql.metamodel.Relation;
import org.finos.legend.engine.protocol.sql.metamodel.SelectItem;
import org.finos.legend.engine.protocol.sql.metamodel.TableFunction;
import org.finos.legend.engine.protocol.sql.metamodel.TableSubquery;

/**
 * Utility class for detecting pass-through SQL queries.
 *
 * A pass-through query is one that returns exactly what the service returns with no modifications.
 */
public class PassThroughQueryDetector
{
    private PassThroughQueryDetector()
    {
    }

    /**
     * Checks if a query is a pass-through query that can use a pre-generated plan.
     *
     * A query is pass-through if:
     * - It's a SELECT * (all columns, no specific selection)
     * - It has exactly one FROM clause that's a service/table function
     * - It has no WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, or OFFSET clauses
     */
    public static boolean isPassThrough(Query query)
    {
        if (query == null || query.queryBody == null)
        {
            return false;
        }

        if (!(query.queryBody instanceof QuerySpecification))
        {
            return false;
        }

        QuerySpecification spec = (QuerySpecification) query.queryBody;

        boolean selectAll = isSelectAll(spec);
        boolean singleSource = hasSingleServiceSource(spec);
        boolean noModifiers = hasNoModifyingClauses(spec);

        return selectAll && singleSource && noModifiers;
    }

    // Checks if the SELECT clause is "SELECT *" (all columns, not DISTINCT).
    private static boolean isSelectAll(QuerySpecification spec)
    {
        if (spec.select == null || spec.select.selectItems == null || spec.select.selectItems.size() != 1)
        {
            return false;
        }

        // DISTINCT modifies the result, so it's not pass-through
        if (spec.select.distinct)
        {
            return false;
        }

        SelectItem item = spec.select.selectItems.get(0);

        if (!(item instanceof AllColumns))
        {
            return false;
        }

        // Must not have a prefix (e.g., SELECT t.* would have prefix "t")
        AllColumns allColumns = (AllColumns) item;
        return allColumns.prefix == null || allColumns.prefix.isEmpty();
    }

    /**
     * Checks if the FROM clause has exactly one source that's a table function (service)
     * or a pass-through subquery (SELECT * FROM (SELECT * FROM service)).
     */
    private static boolean hasSingleServiceSource(QuerySpecification spec)
    {
        if (spec.from == null || spec.from.size() != 1)
        {
            return false;
        }

        Relation source = spec.from.get(0);

        return isPassThroughRelation(source);
    }

    // Checks if a relation is a pass-through source (service reference, aliased service, or pass-through subquery).
    private static boolean isPassThroughRelation(Relation source)
    {
        // Direct service reference: SELECT * FROM service('/myService')
        if (source instanceof TableFunction)
        {
            return true;
        }

        // Aliased relation: SELECT * FROM service('/myService') AS t
        if (source instanceof AliasedRelation)
        {
            AliasedRelation aliased = (AliasedRelation) source;
            return aliased.relation != null && isPassThroughRelation(aliased.relation);
        }

        // Nested subquery: SELECT * FROM (SELECT * FROM service('/myService'))
        if (source instanceof TableSubquery)
        {
            TableSubquery subquery = (TableSubquery) source;
            // Recursively check if the inner query is also pass-through
            return subquery.query != null && isPassThrough(subquery.query);
        }

        return false;
    }


    // Checks that the query has no clauses that would modify the result.
    private static boolean hasNoModifyingClauses(QuerySpecification spec)
    {
        if (spec.where != null)
        {
            return false;
        }

        if (spec.groupBy != null && !spec.groupBy.isEmpty())
        {
            return false;
        }

        if (spec.having != null)
        {
            return false;
        }

        if (spec.orderBy != null && !spec.orderBy.isEmpty())
        {
            return false;
        }

        if (spec.limit != null)
        {
            return false;
        }

        if (spec.offset != null)
        {
            return false;
        }

        return true;
    }
}

