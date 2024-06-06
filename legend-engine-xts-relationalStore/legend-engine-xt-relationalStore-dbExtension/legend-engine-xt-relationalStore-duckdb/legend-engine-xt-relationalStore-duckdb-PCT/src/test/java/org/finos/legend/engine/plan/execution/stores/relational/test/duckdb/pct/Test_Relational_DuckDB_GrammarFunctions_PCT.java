// Copyright 2023 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.plan.execution.stores.relational.test.duckdb.pct;

import junit.framework.Test;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.engine.plan.execution.stores.relational.connection.tests.api.TestConnectionIntegrationLoader;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.DatabaseType;
import org.finos.legend.engine.pure.runtime.testConnection.CoreExternalTestConnectionCodeRepositoryProvider;
import org.finos.legend.engine.test.shared.framework.TestServerResource;
import org.finos.legend.pure.code.core.CoreRelationalduckdbCodeRepositoryProvider;
import org.finos.legend.pure.m3.PlatformCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.compiled.testHelper.PureTestBuilderCompiled;

import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

public class Test_Relational_DuckDB_GrammarFunctions_PCT extends PCTReportConfiguration
{
    private static final ReportScope reportScope = PlatformCodeRepositoryProvider.grammarFunctions;
    private static final Adapter adapter = CoreExternalTestConnectionCodeRepositoryProvider.duckDBAdapter;
    private static final String platform = "compiled";
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // Not
            one("meta::pure::functions::boolean::tests::conjunctions::not::testNotInCollection_Function_1__Boolean_1_", "\"->at(...) function is supported only after direct access of 1->MANY properties. Current expression: [false, false -> not()] -> at(1)\""),

            // Eq
            one("meta::pure::functions::boolean::tests::equality::eq::testEqDate_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_NoCounter, Anonymous_NoCounter, false]\""),
            one("meta::pure::functions::boolean::tests::equality::eq::testEqEnum_Function_1__Boolean_1_", "\"Assert failed\""),
            one("meta::pure::functions::boolean::tests::equality::eq::testEqNonPrimitive_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),
            one("meta::pure::functions::boolean::tests::equality::eq::testEqVarIdentity_Function_1__Boolean_1_", "\"The system is trying to get an element at offset 1 where the collection is of size 1\""),

            // Equal
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualDate_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_NoCounter, Anonymous_NoCounter, false]\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualEnum_Function_1__Boolean_1_", "\"Assert failed\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualNonPrimitive_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),
            one("meta::pure::functions::boolean::tests::equality::equal::testEqualVarIdentity_Function_1__Boolean_1_", "\"The system is trying to get an element at offset 1 where the collection is of size 1\""),

            // GreaterThan
            one("meta::pure::functions::boolean::tests::inequalities::greaterThan::testGreaterThan_Boolean_Function_1__Boolean_1_", "\"Assert failed\""),

            // GreaterThanEqual
            one("meta::pure::functions::boolean::tests::inequalities::greaterThanEqual::testGreaterThanEqual_Boolean_Function_1__Boolean_1_", "\"Assert failed\""),

            // Filter
            one("meta::pure::functions::collection::tests::filter::testFilterInstance_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),
            one("meta::pure::functions::collection::tests::filter::testFilterLiteralFromVar_Function_1__Boolean_1_", "\"Cannot cast a collection of size 3 to multiplicity [1]\""),
            one("meta::pure::functions::collection::tests::filter::testFilterLiteral_Function_1__Boolean_1_", "\"Cannot cast a collection of size 3 to multiplicity [1]\""),
            one("meta::pure::functions::collection::tests::filter::testLambdaAsFunctionParameter_Function_1__Boolean_1_", "\"Cannot cast a collection of size 3 to multiplicity [1]\""),

            // First
            one("meta::pure::functions::collection::tests::first::testFirstComplex_Function_1__Boolean_1_", "\"The system is trying to get an element at offset 0 where the collection is of size 0\""),
            one("meta::pure::functions::collection::tests::first::testFirstOnEmptySet_Function_1__Boolean_1_", "\"Cast exception: SelectSQLQuery cannot be cast to TdsSelectSqlQuery\""),
            one("meta::pure::functions::collection::tests::first::testFirstSimple_Function_1__Boolean_1_", "\"Cannot cast a collection of size 2 to multiplicity [1]\""),

            // Map
            one("meta::pure::functions::collection::tests::map::testMapInstance_Function_1__Boolean_1_", "\"Cannot cast a collection of size 0 to multiplicity [1]\""),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),
            one("meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne_Function_1__Boolean_1_", "\"Error during dynamic reactivation: Error dynamically evaluating value specification (from /platform/pure/grammar/functions/collection/base/map.pure:43cc92-98); error compiling generated Java code:\nimport org.eclipse.collections.api.LazyIterable;\nimport org.eclipse.collections.api.block.function.Function0;\nimport org.eclipse.collections.api.block.function.Function;\nimport org.eclipse.collections.api.block.function.Function2;\nimport org.eclipse.collections.api.block.predicate.Predicate;\nimport org.eclipse.collections.api.block.procedure.Procedure;\nimport org.eclipse.collections.api.map.ImmutableMap;\nimport org.eclipse.collections.api.map.MutableMap;\nimport org.eclipse.collections.api.map.MutableMapIterable;\nimport org.eclipse.collections.api.map.MapIterable;\nimport org.eclipse.collections.api.map.primitive.IntObjectMap;\nimport org.eclipse.collections.api.set.MutableSet;\nimport org.eclipse.collections.api.set.SetIterable;\nimport org.eclipse.collections.api.list.MutableList;\nimport org.eclipse.collections.api.list.ListIterable;\nimport org.eclipse.collections.api.RichIterable;\nimport org.eclipse.collections.api.tuple.Pair;\nimport org.eclipse.collections.impl.factory.Lists;\nimport org.eclipse.collections.impl.factory.Maps;\nimport org.eclipse.collections.impl.map.mutable.UnifiedMap;\nimport org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;\nimport org.eclipse.collections.impl.set.mutable.UnifiedSet;\nimport org.eclipse.collections.impl.set.strategy.mutable.UnifiedSetWithHashingStrategy;\nimport org.eclipse.collections.impl.list.mutable.FastList;\nimport org.eclipse.collections.impl.factory.Sets;\nimport org.eclipse.collections.impl.block.function.checked.CheckedFunction0;\nimport org.eclipse.collections.impl.utility.Iterate;\nimport org.eclipse.collections.impl.utility.LazyIterate;\nimport org.eclipse.collections.impl.utility.StringIterate;\nimport org.finos.legend.pure.m3.navigation.generictype.GenericType;\nimport org.finos.legend.pure.m3.navigation.ProcessorSupport;\nimport org.finos.legend.pure.m3.execution.ExecutionSupport;\nimport org.finos.legend.pure.m3.exception.PureExecutionException;\nimport org.finos.legend.pure.m4.coreinstance.CoreInstance;\nimport org.finos.legend.pure.m4.coreinstance.primitive.date.PureDate;\nimport org.finos.legend.pure.m4.coreinstance.primitive.date.DateFunctions;\nimport org.finos.legend.pure.runtime.java.compiled.generation.processors.support.coreinstance.*;\nimport org.finos.legend.pure.runtime.java.compiled.metadata.*;\nimport org.finos.legend.pure.runtime.java.compiled.generation.processors.support.map.*;\nimport org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.vcs.ChangeType;\nimport org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.vcs.ChangedPath;\nimport org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.vcs.Revision;\nimport org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement;\nimport org.finos.legend.pure.m3.tools.ListHelper;\nimport org.finos.legend.pure.runtime.java.compiled.generation.processors.support.*;\nimport org.finos.legend.pure.runtime.java.compiled.generation.processors.support.function.defended.*;\nimport org.finos.legend.pure.runtime.java.compiled.generation.processors.support.function.*;\nimport org.finos.legend.pure.runtime.java.compiled.execution.*;\nimport org.finos.legend.pure.runtime.java.compiled.execution.sourceInformation.*;\nimport org.finos.legend.pure.runtime.java.compiled.serialization.model.*;\nimport org.finos.legend.pure.runtime.java.compiled.metadata.*;\nimport java.lang.reflect.Method;\nimport java.math.BigInteger;\nimport java.sql.DatabaseMetaData;\nimport java.sql.PreparedStatement;\nimport java.sql.ResultSetMetaData;\nimport java.util.Iterator;\nimport java.util.Calendar;\nimport java.util.Map;\nimport java.util.ArrayDeque;\nimport java.util.Deque;\nimport org.json.simple.JSONObject;\nimport org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.relationship.Generalization;\nimport org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.Type;\n\n\npublic class DynaClass{\n   public static org.finos.legend.pure.generated.Root_meta_pure_functions_collection_tests_map_model_M_Address doProcess(final MapIterable<String, Object> vars, final MutableMap<String, Object> valMap, final IntObjectMap<CoreInstance> localLambdas, final ExecutionSupport es){\n       return _address;\n   }\n}\n\""),

            // Range
            one("meta::pure::functions::collection::tests::range::testRangeWithStep_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'range_Integer_1__Integer_1__Integer_1__Integer_MANY_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),
            one("meta::pure::functions::collection::tests::range::testRangeWithVariables_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'range_Integer_1__Integer_1__Integer_1__Integer_MANY_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),
            one("meta::pure::functions::collection::tests::range::testRange_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'range_Integer_1__Integer_1__Integer_1__Integer_MANY_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),
            one("meta::pure::functions::collection::tests::range::testReverseRange_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'range_Integer_1__Integer_1__Integer_1__Integer_MANY_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),

            // Size
            one("meta::pure::functions::collection::tests::size::testSizeEmpty_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'size_Any_MANY__Integer_1_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),
            one("meta::pure::functions::collection::tests::size::testSize_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'size_Any_MANY__Integer_1_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),

            // Compare
            pack("meta::pure::functions::lang::tests::compare", "\"No SQL translation exists for the PURE function 'compare_T_1__T_1__Integer_1_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),
            one("meta::pure::functions::lang::tests::compare::testCompareDecimalAndLongTypes_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'compare_T_1__T_1__Integer_1_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),
            one("meta::pure::functions::lang::tests::compare::testDateCompare_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'compare_T_1__T_1__Integer_1_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),

            // Divide
            one("meta::pure::functions::math::tests::divide::testDecimalDivide_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'minus_Decimal_MANY__Decimal_1_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),

            // Minus
            one("meta::pure::functions::math::tests::minus::testDecimalMinus_Function_1__Boolean_1_", "\"No SQL translation exists for the PURE function 'minus_Decimal_MANY__Decimal_1_'. \nIf you would like to add a SQL translation for the function then follow the step-by-step guide on the PURE wiki.\""),
            one("meta::pure::functions::math::tests::minus::testLargeMinus_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),
            one("meta::pure::functions::math::tests::minus::testSingleMinusType_Function_1__Boolean_1_", "\"Cast exception: StoreRoutingStrategy cannot be cast to StoreMappingRoutingStrategy\""),
            one("meta::pure::functions::math::tests::minus::testSingleMinus_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),

            // Plus
            one("meta::pure::functions::math::tests::plus::testDecimalPlus_Function_1__Boolean_1_", "class java.lang.Double cannot be cast to class java.math.BigDecimal (java.lang.Double and java.math.BigDecimal are in module java.base of loader 'bootstrap')"),
            one("meta::pure::functions::math::tests::plus::testLargePlus_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),
            one("meta::pure::functions::math::tests::plus::testSinglePlusType_Function_1__Boolean_1_", "\"Cast exception: StoreRoutingStrategy cannot be cast to StoreMappingRoutingStrategy\""),

            // Times
            one("meta::pure::functions::math::tests::times::testDecimalTimes_Function_1__Boolean_1_", "\"\nexpected: 353791.470D\nactual:   353791.47\""),
            one("meta::pure::functions::math::tests::times::testLargeTimes_Function_1__Boolean_1_", "\"Unexpected error executing function with params [Anonymous_Lambda]\""),

            // Plus (String)
            one("meta::pure::functions::string::tests::plus::testMultiPlusWithPropertyExpressions_Function_1__Boolean_1_", "\"Cannot cast a collection of size 0 to multiplicity [1]\""),
            one("meta::pure::functions::string::tests::plus::testPlusInCollect_Function_1__Boolean_1_", "\"Error mapping not found for class P_Person cache:''\""),
            one("meta::pure::functions::string::tests::plus::testPlusInIterate_Function_1__Boolean_1_", "\"Error mapping not found for class P_Person cache:''\"")


    );

    public static Test suite()
    {
        return wrapSuite(
                () -> true,
                () -> PureTestBuilderCompiled.buildPCTTestSuite(reportScope, expectedFailures, adapter),
                () -> false,
                Lists.mutable.with((TestServerResource) TestConnectionIntegrationLoader.extensions().select(c -> c.getDatabaseType() == DatabaseType.DuckDB).getFirst())
        );
    }

    @Override
    public MutableList<ExclusionSpecification> expectedFailures()
    {
        return expectedFailures;
    }

    @Override
    public ReportScope getReportScope()
    {
        return reportScope;
    }

    @Override
    public Adapter getAdapter()
    {
        return adapter;
    }

    @Override
    public String getPlatform()
    {
        return platform;
    }
}
