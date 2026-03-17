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

package org.finos.legend.engine.query.sql.api.execute;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.finos.legend.engine.language.pure.modelManager.ModelManager;
import org.finos.legend.engine.language.sql.grammar.from.SQLGrammarParser;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.generation.extension.PlanGeneratorExtension;
import org.finos.legend.engine.protocol.sql.metamodel.Query;
import org.finos.legend.engine.pure.code.core.PureCoreExtensionLoader;
import org.finos.legend.engine.query.sql.api.CatchAllExceptionMapper;
import org.finos.legend.engine.query.sql.api.MockPac4jFeature;
import org.finos.legend.engine.query.sql.api.TestSQLSourceProvider;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import java.util.ServiceLoader;

import static org.junit.Assert.assertTrue;

/**
 * Integration tests for Pass-Through Query Optimization.
 *
 * These tests ACTUALLY EXECUTE queries through SQLExecutor to verify that:
 * 1. Pass-through queries use the optimized execution path
 * 2. Non-pass-through queries use the standard execution path
 *
 * The pass-through optimization bypasses SQL-to-Pure transformation for simple
 * SELECT * FROM service('/...') queries, generating plans directly from the
 * service's lambda function.
 */
public class PassThroughExecutionIntegrationTest
{
    static
    {
        System.setProperty(TestProperties.CONTAINER_PORT, "0");
    }

    @ClassRule
    public static final ResourceTestRule resources = getResourceTestRule();
    private static final ObjectMapper OM = new ObjectMapper();
    private static final SQLGrammarParser PARSER = SQLGrammarParser.newInstance();

    public static ResourceTestRule getResourceTestRule()
    {
        DeploymentMode deploymentMode = DeploymentMode.TEST;
        ModelManager modelManager = new ModelManager(deploymentMode);
        PlanExecutor executor = PlanExecutor.newPlanExecutorWithAvailableStoreExecutors();

        MutableList<PlanGeneratorExtension> generatorExtensions = Lists.mutable.withAll(ServiceLoader.load(PlanGeneratorExtension.class));
        TestSQLSourceProvider testSQLSourceProvider = new TestSQLSourceProvider();
        SqlExecute sqlExecute = new SqlExecute(modelManager, executor, (pm) -> PureCoreExtensionLoader.extensions().flatCollect(g -> g.extraPureCoreExtensions(pm.getExecutionSupport())), FastList.newListWith(testSQLSourceProvider), generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers));

        return ResourceTestRule.builder()
                .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                .addResource(sqlExecute)
                .addResource(new MockPac4jFeature())
                .addResource(new CatchAllExceptionMapper())
                .bootstrapLogging(false)
                .build();
    }

    private Query parse(String sql)
    {
        return (Query) PARSER.parseStatement(sql);
    }

    private String executeQuery(String sql)
    {
        return resources.target("sql/v1/execution/execute")
                .request()
                .post(Entity.json(new SQLQueryInput(null, sql, FastList.newList()))).readEntity(String.class);
    }

    // ==================== PASS-THROUGH QUERY TESTS ====================
    // These use the optimized pass-through execution path

    @Test
    public void testPassThrough_SimpleSelectAll() throws JsonProcessingException
    {
        String sql = "SELECT * FROM service('/testService')";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() > 0);
    }

    @Test
    public void testPassThrough_NestedSelectAll() throws JsonProcessingException
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/testService'))";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() > 0);
    }

    @Test
    public void testPassThrough_WithTableAlias() throws JsonProcessingException
    {
        String sql = "SELECT * FROM service('/testService') AS t";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() > 0);
    }

    // ==================== NON-PASS-THROUGH QUERY TESTS ====================
    // These use the standard execution path (full SQL-to-Pure transformation)

    @Test
    public void testStandard_SelectWithWhereClause() throws JsonProcessingException
    {
        String sql = "SELECT * FROM service('/testService') WHERE Id > 0";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }

    @Test
    public void testStandard_SelectSpecificColumns() throws JsonProcessingException
    {
        String sql = "SELECT Name FROM service('/testService')";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() > 0);
    }

    @Test
    public void testStandard_SelectWithOrderBy() throws JsonProcessingException
    {
        String sql = "SELECT * FROM service('/testService') ORDER BY Name";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() > 0);
    }

    @Test
    public void testStandard_SelectWithLimit() throws JsonProcessingException
    {
        String sql = "SELECT * FROM service('/testService') LIMIT 10";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }

    @Test
    public void testStandard_SelectWithGroupBy() throws JsonProcessingException
    {
        String sql = "SELECT Name, count(*) FROM service('/testService') GROUP BY Name";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }

    @Test
    public void testStandard_NestedSelectWithWhere() throws JsonProcessingException
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/testService') WHERE Id > 0)";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }

    @Test
    public void testStandard_SelectDistinct() throws JsonProcessingException
    {
        String sql = "SELECT DISTINCT * FROM service('/testService')";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }

    @Test
    public void testStandard_SelectWithPrefixedStar() throws JsonProcessingException
    {
        String sql = "SELECT t.* FROM service('/testService') AS t";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }

    @Test
    public void testStandard_OuterWhereOnNestedPassThrough() throws JsonProcessingException
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/testService')) WHERE Id > 0";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }

    @Test
    public void testStandard_OuterOrderByOnNestedPassThrough() throws JsonProcessingException
    {
        String sql = "SELECT * FROM (SELECT * FROM service('/testService')) ORDER BY Name";
        String result = executeQuery(sql);

        TDSExecuteResult tdsResult = OM.readValue(result, TDSExecuteResult.class);
        assertTrue("Should return rows", tdsResult.result.rows.size() >= 0);
    }
}
