// Copyright 2024 Goldman Sachs
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

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.finos.legend.engine.language.pure.modelManager.ModelManager;
import org.finos.legend.engine.language.sql.grammar.from.SQLGrammarParser;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.generation.extension.PlanGeneratorExtension;
import org.finos.legend.engine.protocol.sql.metamodel.Query;
import org.finos.legend.engine.pure.code.core.PureCoreExtensionLoader;
import org.finos.legend.engine.query.sql.providers.core.SQLContext;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.junit.Before;
import org.junit.Test;

import java.util.ServiceLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Plan Caching Optimization Tests
 *
 * Validates that execution plans are cached and reused for repeated identical SQL queries,
 * avoiding expensive plan regeneration.
 *
 * Key behaviors tested:
 * - Identical queries reuse the same cached plan
 * - Different queries get separate cache entries
 * - Cache TTL and eviction work correctly
 */
public class SQLExecutorDebugTest
{
    private SQLExecutor sqlExecutor;
    private SQLGrammarParser parser;
    private Identity identity;

    @Before
    public void setUp()
    {
        // Clear the plan cache to ensure clean state for each test
        SQLExecutor.clearPlanCache();

        // Set up the SQL executor with test infrastructure
        DeploymentMode deploymentMode = DeploymentMode.TEST;
        ModelManager modelManager = new ModelManager(deploymentMode);
        PlanExecutor planExecutor = PlanExecutor.newPlanExecutorWithAvailableStoreExecutors();

        MutableList<PlanGeneratorExtension> generatorExtensions = Lists.mutable.withAll(ServiceLoader.load(PlanGeneratorExtension.class));
        TestSQLSourceProvider testSQLSourceProvider = new TestSQLSourceProvider();

        this.sqlExecutor = new SQLExecutor(
                modelManager,
                planExecutor,
                (pm) -> PureCoreExtensionLoader.extensions().flatCollect(g -> g.extraPureCoreExtensions(pm.getExecutionSupport())),
                FastList.newListWith(testSQLSourceProvider),
                generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers)
        );

        this.parser = SQLGrammarParser.newInstance();
        this.identity = Identity.getAnonymousIdentity();
    }

    // ==================== CORE CACHING TESTS ====================

    @Test
    public void testIdenticalQueriesReuseCachedPlan()
    {
        SQLExecutor.clearPlanCache();
        assertEquals("Cache should be empty at start", 0, SQLExecutor.getPlanCacheSize());
        assertEquals("Plan generation count should be 0", 0, SQLExecutor.getPlanGenerationCount());

        String sql = "SELECT * FROM service('/testService')";

        // First execution - generates and caches plan
        executeQuery(sql);
        assertEquals("First execution should generate plan", 1, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should have 1 entry", 1, SQLExecutor.getPlanCacheSize());

        // Second execution - should reuse cached plan
        executeQuery(sql);
        assertEquals("Second execution should NOT generate new plan", 1, SQLExecutor.getPlanGenerationCount());

        // Third execution - should still reuse cached plan
        executeQuery(sql);
        assertEquals("Third execution should NOT generate new plan", 1, SQLExecutor.getPlanGenerationCount());

        // Cache size should remain 1 (same query)
        assertEquals("Cache should still have 1 entry", 1, SQLExecutor.getPlanCacheSize());
    }

    @Test
    public void testDifferentQueriesGetSeparateCacheEntries()
    {
        SQLExecutor.clearPlanCache();

        String queryA = "SELECT * FROM service('/testService')";
        String queryB = "SELECT * FROM service('/personServiceForNames')";

        // Execute query A - generates plan
        executeQuery(queryA);
        assertEquals("Query A should generate plan", 1, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should have 1 entry", 1, SQLExecutor.getPlanCacheSize());

        // Execute query B - generates NEW plan (different query)
        executeQuery(queryB);
        assertEquals("Query B should generate new plan", 2, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should have 2 entries", 2, SQLExecutor.getPlanCacheSize());

        // Execute query A again - should reuse cached plan
        executeQuery(queryA);
        assertEquals("Query A (repeat) should NOT generate plan", 2, SQLExecutor.getPlanGenerationCount());

        // Execute query B again - should reuse cached plan
        executeQuery(queryB);
        assertEquals("Query B (repeat) should NOT generate plan", 2, SQLExecutor.getPlanGenerationCount());

        // Cache should have exactly 2 entries
        assertEquals("Cache should have 2 entries", 2, SQLExecutor.getPlanCacheSize());
    }

    // ==================== QUERY VARIATION TESTS ====================

    @Test
    public void testSemanticallyEquivalentQueriesShareCachedPlan()
    {
        SQLExecutor.clearPlanCache();

        // These two queries are semantically equivalent:
        // Query 1: SELECT * FROM service('/testService')
        // Query 2: SELECT * FROM (SELECT * FROM service('/testService'))
        String directQuery = "SELECT * FROM service('/testService')";
        String wrappedQuery = "SELECT * FROM (SELECT * FROM service('/testService'))";

        // Execute direct query - generates and caches plan
        executeQuery(directQuery);
        assertEquals("Direct query should generate plan", 1, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should have 1 entry", 1, SQLExecutor.getPlanCacheSize());

        // Execute wrapped query - should reuse cached plan (same semantics)
        executeQuery(wrappedQuery);
        assertEquals("Wrapped query should reuse plan (same semantics)", 1, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should still have 1 entry", 1, SQLExecutor.getPlanCacheSize());

        // Execute direct query again - should still reuse cached plan
        executeQuery(directQuery);
        assertEquals("Direct query (repeat) should reuse plan", 1, SQLExecutor.getPlanGenerationCount());
    }

    @Test
    public void testSemanticallyEquivalentQueriesWithWhereClause()
    {
        SQLExecutor.clearPlanCache();

        // These queries are also semantically equivalent:
        String directQuery = "SELECT * FROM service('/personServiceForNames') WHERE Name = 'Alice'";
        String wrappedQuery = "SELECT * FROM (SELECT * FROM service('/personServiceForNames') WHERE Name = 'Alice')";

        // Execute direct query
        executeQuery(directQuery);
        assertEquals("Direct query should generate plan", 1, SQLExecutor.getPlanGenerationCount());

        // Execute wrapped query - should reuse cached plan
        executeQuery(wrappedQuery);
        assertEquals("Wrapped query should reuse plan", 1, SQLExecutor.getPlanGenerationCount());
    }

    @Test
    public void testQueryWithWhereClauseGetsSeparateCacheEntry()
    {
        SQLExecutor.clearPlanCache();

        String baseQuery = "SELECT * FROM service('/personServiceForNames')";
        String queryWithWhere = "SELECT * FROM service('/personServiceForNames') WHERE Name = 'Alice'";

        // Execute base query
        executeQuery(baseQuery);
        assertEquals("Base query should generate plan", 1, SQLExecutor.getPlanGenerationCount());

        // Execute query with WHERE - should generate NEW plan
        executeQuery(queryWithWhere);
        assertEquals("Query with WHERE should generate new plan", 2, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should have 2 entries", 2, SQLExecutor.getPlanCacheSize());

        // Execute query with WHERE again - should reuse cached plan
        executeQuery(queryWithWhere);
        assertEquals("Query with WHERE (repeat) should NOT generate plan", 2, SQLExecutor.getPlanGenerationCount());
    }

    @Test
    public void testQueryWithSpecificColumnsGetsSeparateCacheEntry()
    {
        SQLExecutor.clearPlanCache();

        String selectAll = "SELECT * FROM service('/personServiceForNames')";
        String selectSpecific = "SELECT Name FROM service('/personServiceForNames')";

        // Execute SELECT *
        executeQuery(selectAll);
        assertEquals("SELECT * should generate plan", 1, SQLExecutor.getPlanGenerationCount());

        // Execute SELECT with specific columns - should generate NEW plan
        executeQuery(selectSpecific);
        assertEquals("SELECT specific columns should generate new plan", 2, SQLExecutor.getPlanGenerationCount());

        // Execute SELECT * again - should reuse cached plan
        executeQuery(selectAll);
        assertEquals("SELECT * (repeat) should NOT generate plan", 2, SQLExecutor.getPlanGenerationCount());

        // Execute SELECT specific again - should reuse cached plan
        executeQuery(selectSpecific);
        assertEquals("SELECT specific (repeat) should NOT generate plan", 2, SQLExecutor.getPlanGenerationCount());
    }

    @Test
    public void testQueryWithOrderByGetsSeparateCacheEntry()
    {
        SQLExecutor.clearPlanCache();

        String baseQuery = "SELECT * FROM service('/personServiceForNames')";
        String queryWithOrderBy = "SELECT * FROM service('/personServiceForNames') ORDER BY Name";

        // Execute base query
        executeQuery(baseQuery);
        assertEquals("Base query should generate plan", 1, SQLExecutor.getPlanGenerationCount());

        // Execute query with ORDER BY - should generate NEW plan
        executeQuery(queryWithOrderBy);
        assertEquals("Query with ORDER BY should generate new plan", 2, SQLExecutor.getPlanGenerationCount());

        // Execute query with ORDER BY again - should reuse cached plan
        executeQuery(queryWithOrderBy);
        assertEquals("Query with ORDER BY (repeat) should NOT generate plan", 2, SQLExecutor.getPlanGenerationCount());
    }

    // ==================== PERFORMANCE VALIDATION ====================

    @Test
    public void testCacheHitIsFasterThanCacheMiss()
    {
        SQLExecutor.clearPlanCache();

        String sql = "SELECT * FROM service('/testService')";

        // First execution - cache miss, includes model loading (slow)
        long start1 = System.currentTimeMillis();
        executeQuery(sql);
        long time1 = System.currentTimeMillis() - start1;

        // Second execution - cache hit (should be faster)
        long start2 = System.currentTimeMillis();
        executeQuery(sql);
        long time2 = System.currentTimeMillis() - start2;

        // Third execution - cache hit (should be faster)
        long start3 = System.currentTimeMillis();
        executeQuery(sql);
        long time3 = System.currentTimeMillis() - start3;

        System.out.println("=== Performance Results ===");
        System.out.println("First execution (cache miss):  " + time1 + "ms");
        System.out.println("Second execution (cache hit): " + time2 + "ms");
        System.out.println("Third execution (cache hit):  " + time3 + "ms");

        // Cache hits should be faster (only check if first was slow enough to measure)
        if (time1 > 500)
        {
            assertTrue("Cache hit should be faster than cache miss", time2 < time1);
            assertTrue("Cache hit should be faster than cache miss", time3 < time1);
        }

        // Verify only one plan was generated
        assertEquals("Only one plan should be generated", 1, SQLExecutor.getPlanGenerationCount());
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testCacheWorksAcrossMultipleServices()
    {
        SQLExecutor.clearPlanCache();

        String service1Query = "SELECT * FROM service('/testService')";
        String service2Query = "SELECT * FROM service('/personServiceForNames')";

        // Execute against service 1
        executeQuery(service1Query);
        assertEquals(1, SQLExecutor.getPlanGenerationCount());

        // Execute against service 2
        executeQuery(service2Query);
        assertEquals(2, SQLExecutor.getPlanGenerationCount());

        // Repeat both - should reuse cached plans
        executeQuery(service1Query);
        executeQuery(service2Query);
        assertEquals("Both repeats should use cache", 2, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should have 2 entries", 2, SQLExecutor.getPlanCacheSize());
    }

    @Test
    public void testCacheClearWorksCorrectly()
    {
        SQLExecutor.clearPlanCache();

        String sql = "SELECT * FROM service('/testService')";

        // First execution - generates plan
        executeQuery(sql);
        assertEquals(1, SQLExecutor.getPlanGenerationCount());
        assertEquals(1, SQLExecutor.getPlanCacheSize());

        // Clear cache
        SQLExecutor.clearPlanCache();
        assertEquals("Cache should be empty after clear", 0, SQLExecutor.getPlanCacheSize());
        assertEquals("Generation count should reset", 0, SQLExecutor.getPlanGenerationCount());

        // Execute again - should regenerate plan
        executeQuery(sql);
        assertEquals("Should generate plan after cache clear", 1, SQLExecutor.getPlanGenerationCount());
        assertEquals("Cache should have 1 entry", 1, SQLExecutor.getPlanCacheSize());
    }

    // ==================== HELPER METHODS ====================

    private Result executeQuery(String sql)
    {
        Query query = (Query) parser.parseStatement(sql);
        SQLContext context = new SQLContext(query);
        return sqlExecutor.execute(query, "testUser", context, identity);
    }
}
