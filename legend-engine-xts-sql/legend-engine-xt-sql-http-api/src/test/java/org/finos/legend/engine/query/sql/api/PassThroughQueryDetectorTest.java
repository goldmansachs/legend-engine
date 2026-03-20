// Copyright 2026 Goldman Sachs
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

import org.finos.legend.engine.language.sql.grammar.from.SQLGrammarParser;
import org.finos.legend.engine.protocol.sql.metamodel.Query;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for PassThroughQueryDetector.
 *
 * A pass-through query is a SELECT * query with no modifications (no WHERE, ORDER BY, etc.)
 * that can use a pre-generated execution plan directly without SQL-to-Pure transformation.
 */
public class PassThroughQueryDetectorTest
{
    private static final SQLGrammarParser PARSER = SQLGrammarParser.newInstance();

    private Query parse(String sql)
    {
        return (Query) PARSER.parseStatement(sql);
    }

    // ==================== PASS-THROUGH QUERIES (should return true) ====================

    @Test
    public void testSimpleSelectAll_IsPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService')");
        assertTrue("Simple SELECT * should be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testNestedSelectAll_IsPassThrough()
    {
        Query query = parse("SELECT * FROM (SELECT * FROM service('/myService'))");
        assertTrue("Nested SELECT * should be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testDeeplyNestedSelectAll_IsPassThrough()
    {
        Query query = parse("SELECT * FROM (SELECT * FROM (SELECT * FROM service('/myService')))");
        assertTrue("Deeply nested SELECT * should be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testSelectAllWithTableAlias_IsPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService') AS t");
        assertTrue("SELECT * with table alias should be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testNestedSelectAllWithAlias_IsPassThrough()
    {
        Query query = parse("SELECT * FROM (SELECT * FROM service('/myService') AS inner_t) AS outer_t");
        assertTrue("Nested SELECT * with aliases should be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== NON-PASS-THROUGH - WHERE CLAUSE ====================

    @Test
    public void testSelectWithWhere_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService') WHERE age > 30");
        assertFalse("SELECT * with WHERE should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testNestedSelectWithInnerWhere_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM (SELECT * FROM service('/myService') WHERE age > 30)");
        assertFalse("Nested SELECT * with inner WHERE should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testNestedSelectWithOuterWhere_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM (SELECT * FROM service('/myService')) WHERE age > 30");
        assertFalse("Nested SELECT * with outer WHERE should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== NON-PASS-THROUGH - SPECIFIC COLUMNS ====================

    @Test
    public void testSelectSpecificColumn_IsNotPassThrough()
    {
        Query query = parse("SELECT name FROM service('/myService')");
        assertFalse("SELECT specific column should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testSelectMultipleColumns_IsNotPassThrough()
    {
        Query query = parse("SELECT name, age FROM service('/myService')");
        assertFalse("SELECT multiple columns should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testSelectPrefixedStar_IsNotPassThrough()
    {
        Query query = parse("SELECT t.* FROM service('/myService') AS t");
        assertFalse("SELECT t.* (prefixed star) should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testNestedSelectSpecificColumns_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM (SELECT name FROM service('/myService'))");
        assertFalse("Outer SELECT * with inner specific columns should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== NON-PASS-THROUGH - ORDER BY ====================

    @Test
    public void testSelectWithOrderBy_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService') ORDER BY name");
        assertFalse("SELECT * with ORDER BY should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testNestedSelectWithOuterOrderBy_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM (SELECT * FROM service('/myService')) ORDER BY name");
        assertFalse("Nested SELECT * with outer ORDER BY should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== NON-PASS-THROUGH - LIMIT/OFFSET ====================

    @Test
    public void testSelectWithLimit_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService') LIMIT 10");
        assertFalse("SELECT * with LIMIT should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testSelectWithOffset_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService') OFFSET 5");
        assertFalse("SELECT * with OFFSET should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== NON-PASS-THROUGH - GROUP BY/HAVING ====================

    @Test
    public void testSelectWithGroupBy_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService') GROUP BY name");
        assertFalse("SELECT * with GROUP BY should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testSelectWithHaving_IsNotPassThrough()
    {
        Query query = parse("SELECT name, count(*) FROM service('/myService') GROUP BY name HAVING count(*) > 1");
        assertFalse("SELECT with HAVING should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== NON-PASS-THROUGH - DISTINCT ====================

    @Test
    public void testSelectDistinct_IsNotPassThrough()
    {
        Query query = parse("SELECT DISTINCT * FROM service('/myService')");
        assertFalse("SELECT DISTINCT * should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== NON-PASS-THROUGH - MULTIPLE SOURCES ====================

    @Test
    public void testSelectWithJoin_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService') JOIN service('/otherService') ON 1=1");
        assertFalse("SELECT * with JOIN should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    @Test
    public void testSelectFromMultipleSources_IsNotPassThrough()
    {
        Query query = parse("SELECT * FROM service('/myService'), service('/otherService')");
        assertFalse("SELECT * from multiple sources should NOT be pass-through", PassThroughQueryDetector.isPassThrough(query));
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testNullQuery_IsNotPassThrough()
    {
        assertFalse("null query should NOT be pass-through", PassThroughQueryDetector.isPassThrough(null));
    }
}