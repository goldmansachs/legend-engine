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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Performance test to measure the overhead of pass-through query detection.
 *
 * This test measures the time taken for PassThroughQueryDetector.isPassThrough()
 * to determine if a query can use the optimized pass-through execution path.
 *
 * The pass-through optimization works as follows:
 * 1. For SELECT * queries with no modifications (WHERE, ORDER BY, etc.),
 *    the system retrieves a pre-generated execution plan from the service registry
 *    (via AlloyServiceState.getActiveExecutionPlanByUriTemplate())
 * 2. If a pre-generated plan exists, it is executed directly
 * 3. If no plan exists, the system falls back to the standard execution path
 *
 * To measure actual execution time improvement, run with a real database and compare:
 * - SELECT * FROM service('/myService')  → Uses pass-through path (if pre-generated plan exists)
 * - SELECT * FROM service('/myService') WHERE 1=1 → Uses standard path
 *
 * Metrics to monitor:
 * - execute_passthrough: Time for pass-through queries using pre-generated plans
 * - execute_standard: Time for standard queries (with SQL transformation)
 *
 * Expected improvement: 200-450ms saved per query when using pre-generated plans
 * due to skipping:
 * - Model loading (PureModel)
 * - QueryRealiaser.realias()
 * - ProtocolToMetamodelTranslator.translate()
 * - SQLSourceTranslator.translate()
 * - rootContext() (Pure-side context creation)
 * - processRootQuery() (Pure-side SQL merging)
 * - Plan generation
 */
public class PassThroughPerformanceTest
{
    private static final SQLGrammarParser PARSER = SQLGrammarParser.newInstance();
    private static final int WARMUP_ITERATIONS = 100;
    private static final int TEST_ITERATIONS = 1000;

    /**
     * Tests the overhead of pass-through detection.
     * This should be minimal (< 1ms per query) to ensure the optimization is worthwhile.
     */
    @Test
    public void testPassThroughDetectionOverhead()
    {
        // Test queries
        List<String> passThroughQueries = Arrays.asList(
            "SELECT * FROM service('/myService')",
            "SELECT * FROM (SELECT * FROM service('/myService'))",
            "SELECT * FROM service('/myService') AS t"
        );

        List<String> nonPassThroughQueries = Arrays.asList(
            "SELECT * FROM service('/myService') WHERE age > 30",
            "SELECT name FROM service('/myService')",
            "SELECT * FROM service('/myService') ORDER BY name"
        );

        // Warmup
        System.out.println("Warming up pass-through detection...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++)
        {
            for (String sql : passThroughQueries)
            {
                Query query = parse(sql);
                PassThroughQueryDetector.isPassThrough(query);
            }
            for (String sql : nonPassThroughQueries)
            {
                Query query = parse(sql);
                PassThroughQueryDetector.isPassThrough(query);
            }
        }

        // Measure pass-through detection time
        List<Long> detectionTimes = new ArrayList<>();

        System.out.println("Measuring pass-through detection overhead (" + TEST_ITERATIONS + " iterations)...");
        for (int i = 0; i < TEST_ITERATIONS; i++)
        {
            for (String sql : passThroughQueries)
            {
                Query query = parse(sql);
                long start = System.nanoTime();
                boolean isPassThrough = PassThroughQueryDetector.isPassThrough(query);
                long elapsed = System.nanoTime() - start;
                detectionTimes.add(elapsed);
                assertTrue("Expected pass-through for: " + sql, isPassThrough);
            }

            for (String sql : nonPassThroughQueries)
            {
                Query query = parse(sql);
                long start = System.nanoTime();
                boolean isPassThrough = PassThroughQueryDetector.isPassThrough(query);
                long elapsed = System.nanoTime() - start;
                detectionTimes.add(elapsed);
                assertFalse("Expected NOT pass-through for: " + sql, isPassThrough);
            }
        }

        // Calculate statistics
        long totalNanos = detectionTimes.stream().mapToLong(Long::longValue).sum();
        double avgNanos = totalNanos / (double) detectionTimes.size();
        double avgMicros = avgNanos / 1000.0;
        double avgMillis = avgMicros / 1000.0;

        long maxNanos = detectionTimes.stream().mapToLong(Long::longValue).max().orElse(0);
        long minNanos = detectionTimes.stream().mapToLong(Long::longValue).min().orElse(0);

        System.out.println("");
        System.out.println("========== PASS-THROUGH DETECTION PERFORMANCE ==========");
        System.out.println("  Average: " + String.format("%.3f", avgMicros) + " microseconds (" + String.format("%.6f", avgMillis) + " ms)");
        System.out.println("  Min: " + minNanos + " ns, Max: " + maxNanos + " ns");
        System.out.println("  Total detections: " + detectionTimes.size());
        System.out.println("=========================================================");
        System.out.println("");

        // Assert detection overhead is minimal (< 1ms average)
        assertTrue("Pass-through detection should take less than 1ms on average", avgMillis < 1.0);

        // Assert detection overhead is very minimal (< 100 microseconds average)
        assertTrue("Pass-through detection should take less than 100 microseconds on average", avgMicros < 100.0);
    }

    /**
     * Compares parsing overhead for different query types.
     */
    @Test
    public void testQueryParsingOverhead()
    {
        String simpleQuery = "SELECT * FROM service('/myService')";
        String complexQuery = "SELECT * FROM service('/myService') WHERE age > 30 AND name LIKE '%test%' ORDER BY id LIMIT 100";

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++)
        {
            parse(simpleQuery);
            parse(complexQuery);
        }

        // Measure simple query parsing
        long simpleTotal = 0;
        for (int i = 0; i < TEST_ITERATIONS; i++)
        {
            long start = System.nanoTime();
            parse(simpleQuery);
            simpleTotal += System.nanoTime() - start;
        }
        double simpleAvgMicros = (simpleTotal / (double) TEST_ITERATIONS) / 1000.0;

        // Measure complex query parsing
        long complexTotal = 0;
        for (int i = 0; i < TEST_ITERATIONS; i++)
        {
            long start = System.nanoTime();
            parse(complexQuery);
            complexTotal += System.nanoTime() - start;
        }
        double complexAvgMicros = (complexTotal / (double) TEST_ITERATIONS) / 1000.0;

        System.out.println("");
        System.out.println("========== QUERY PARSING PERFORMANCE ==========");
        System.out.println("  Simple query average: " + String.format("%.3f", simpleAvgMicros) + " microseconds");
        System.out.println("  Complex query average: " + String.format("%.3f", complexAvgMicros) + " microseconds");
        System.out.println("  Difference: " + String.format("%.3f", complexAvgMicros - simpleAvgMicros) + " microseconds");
        System.out.println("===============================================");
        System.out.println("");
    }

    private Query parse(String sql)
    {
        return (Query) PARSER.parseStatement(sql);
    }
}

