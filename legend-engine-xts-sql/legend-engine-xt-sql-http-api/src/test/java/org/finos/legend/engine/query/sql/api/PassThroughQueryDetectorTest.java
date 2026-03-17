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

import org.finos.legend.engine.language.sql.grammar.from.SQLGrammarParser;
import org.finos.legend.engine.protocol.sql.metamodel.Query;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PassThroughQueryDetectorTest
{
    private final SQLGrammarParser parser = SQLGrammarParser.newInstance();

    // ==================== PASS-THROUGH QUERIES (should return true) ====================

    @Test
    public void testSimpleSelectAllIsPassThrough()
    {
        String sql = "SELECT * FROM service('/myService')";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertTrue("SELECT * FROM service should be pass-through", result);
    }

    @Test
    public void testNestedSelectAllIsPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/myService'))";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertTrue("SELECT * FROM (SELECT * FROM service) should be pass-through", result);
    }

    @Test
    public void testDeeplyNestedSelectAllIsPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM (SELECT * FROM service('/myService')))";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertTrue("Deeply nested SELECT * FROM service should be pass-through", result);
    }

    // ==================== NON-PASS-THROUGH QUERIES (should return false) ====================

    @Test
    public void testSelectWithWhereClauseIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') WHERE age > 30";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with WHERE clause should NOT be pass-through", result);
    }

    @Test
    public void testSelectSpecificColumnsIsNotPassThrough()
    {
        String sql = "SELECT name FROM service('/myService')";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT specific columns should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithOrderByIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') ORDER BY name";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with ORDER BY should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithLimitIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') LIMIT 10";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with LIMIT should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithGroupByIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') GROUP BY name";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with GROUP BY should NOT be pass-through", result);
    }

    @Test
    public void testSelectMultipleColumnsIsNotPassThrough()
    {
        String sql = "SELECT name, age FROM service('/myService')";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT multiple specific columns should NOT be pass-through", result);
    }

    @Test
    public void testNestedSelectWithWhereIsNotPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/myService') WHERE age > 30)";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("Nested SELECT with WHERE should NOT be pass-through", result);
    }

    @Test
    public void testNestedSelectSpecificColumnsIsNotPassThrough()
    {
        String sql = "SELECT * FROM (SELECT name FROM service('/myService'))";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("Nested SELECT with specific columns should NOT be pass-through", result);
    }

    @Test
    public void testOuterWhereWithNestedPassThroughIsNotPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/myService')) WHERE age > 30";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("Outer WHERE clause makes it NOT pass-through", result);
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testNullQueryIsNotPassThrough()
    {
        boolean result = PassThroughQueryDetector.isPassThrough(null);
        assertFalse("Null query should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithHavingIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') GROUP BY name HAVING count(*) > 1";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with HAVING should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithOffsetIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') OFFSET 5";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with OFFSET should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithLimitAndOffsetIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') LIMIT 10 OFFSET 5";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with LIMIT and OFFSET should NOT be pass-through", result);
    }

    @Test
    public void testSelectDistinctIsNotPassThrough()
    {
        String sql = "SELECT DISTINCT * FROM service('/myService')";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT DISTINCT * should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithTableAliasStarIsNotPassThrough()
    {
        // SELECT t.* is different from SELECT * - it has a prefix
        String sql = "SELECT t.* FROM service('/myService') AS t";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT t.* (prefixed) should NOT be pass-through", result);
    }

    @Test
    public void testSelectWithJoinIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService') JOIN service('/otherService') ON 1=1";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * with JOIN should NOT be pass-through", result);
    }

    @Test
    public void testSelectFromMultipleServicesIsNotPassThrough()
    {
        String sql = "SELECT * FROM service('/myService'), service('/otherService')";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("SELECT * from multiple services should NOT be pass-through", result);
    }

    @Test
    public void testNestedSelectWithOrderByIsNotPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/myService') ORDER BY name)";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("Nested SELECT with ORDER BY should NOT be pass-through", result);
    }

    @Test
    public void testNestedSelectWithLimitIsNotPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/myService') LIMIT 10)";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("Nested SELECT with LIMIT should NOT be pass-through", result);
    }

    @Test
    public void testOuterOrderByWithNestedPassThroughIsNotPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/myService')) ORDER BY name";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("Outer ORDER BY makes it NOT pass-through", result);
    }

    @Test
    public void testOuterLimitWithNestedPassThroughIsNotPassThrough()
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/myService')) LIMIT 10";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertFalse("Outer LIMIT makes it NOT pass-through", result);
    }

    @Test
    public void testSelectAllWithTableAliasIsPassThrough()
    {
        // SELECT * FROM service AS t should still be pass-through (no prefix on *)
        String sql = "SELECT * FROM service('/myService') AS t";
        Query query = parse(sql);
        boolean result = PassThroughQueryDetector.isPassThrough(query);
        assertTrue("SELECT * with table alias (no prefix) should be pass-through", result);
    }

    // ==================== HELPER METHODS ====================

    private Query parse(String sql)
    {
        return (Query) parser.parseStatement(sql);
    }
}

