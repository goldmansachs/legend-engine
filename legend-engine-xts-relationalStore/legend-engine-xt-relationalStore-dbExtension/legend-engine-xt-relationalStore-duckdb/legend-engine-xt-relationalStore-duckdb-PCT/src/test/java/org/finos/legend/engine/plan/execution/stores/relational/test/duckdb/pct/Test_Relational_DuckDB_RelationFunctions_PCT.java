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
import org.finos.legend.engine.test.shared.framework.TestServerResource;
import org.finos.legend.pure.code.core.CoreRelationalDuckDBPCTCodeRepositoryProvider;
import org.finos.legend.pure.code.core.RelationCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.compiled.testHelper.PureTestBuilderCompiled;

import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

public class Test_Relational_DuckDB_RelationFunctions_PCT extends PCTReportConfiguration
{
    private static final ReportScope reportScope = RelationCodeRepositoryProvider.relationFunctions;
    private static final Adapter adapter = CoreRelationalDuckDBPCTCodeRepositoryProvider.duckDBAdapter;
    private static final String platform = "compiled";
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.with(
            // calculated interval
            one("meta::pure::functions::relation::tests::composition::testExtendLeadAdjustDerivedOffset_Function_1__Boolean_1_", "\"\nexpected: '#TDS\n   id,fromDate,seq,toDate,toDateSeq\n   1,2026-01-07T00:00:00.000+0000,1,2026-01-07T00:00:00.000+0000,2026-01-07T00:00:01.000+0000\n   1,2026-01-07T00:00:00.000+0000,2,2026-01-08T00:00:00.000+0000,2026-01-08T00:00:00.000+0000\n   1,2026-01-08T00:00:00.000+0000,1,2026-01-08T00:00:00.000+0000,2026-01-08T00:00:01.000+0000\n   1,2026-01-08T00:00:00.000+0000,2,1816-03-29T05:56:08.067+0000,1816-03-29T05:56:08.067+0000\n#'\nactual:   '#TDS\n   id,fromDate,seq,toDate,toDateSeq\n   1,2026-01-07T00:00:00.000+0000,1,2026-01-07T00:00:00.000+0000,2026-01-07T00:00:01.000+0000\n   1,2026-01-07T00:00:00.000+0000,2,2026-01-08T00:00:00.000+0000,2026-01-08T00:00:00.000+0000\n   1,2026-01-08T00:00:00.000+0000,1,2026-01-08T00:00:00.000+0000,2026-01-08T00:00:01.000+0000\n   1,2026-01-08T00:00:00.000+0000,2,1816-03-29T05:56:09.067+0000,1816-03-29T05:56:09.067+0000\n#'\""),
            one("meta::pure::functions::relation::tests::composition::testProjectExtendNestedIfLeadAdjust_Function_1__Boolean_1_", "\"\nexpected: '#TDS\n   grp,dateSeq,endDate,startDate,endDateSeq\n   1,2026-01-07T00:00:00.000+0000,2026-01-08T00:00:00.000+0000,2026-01-07T00:00:00.000+0000,2026-01-08T00:00:00.000+0000\n   1,2026-01-08T00:00:00.000+0000,2026-01-10T00:00:00.000+0000,2026-01-08T00:00:00.000+0000,2026-01-10T00:00:00.000+0000\n   1,2026-01-09T00:00:00.000+0000,2026-01-09T00:00:00.000+0000,2026-01-09T00:00:00.000+0000,2026-01-09T00:00:01.000+0000\n   1,2026-01-10T00:00:00.000+0000,1816-03-29T05:56:08.067+0000,2026-01-10T00:00:00.000+0000,1816-03-29T05:56:08.067+0000\n#'\nactual:   '#TDS\n   grp,dateSeq,endDate,startDate,endDateSeq\n   1,2026-01-07T00:00:00.000+0000,2026-01-08T00:00:00.000+0000,2026-01-07T00:00:00.000+0000,2026-01-08T00:00:00.000+0000\n   1,2026-01-08T00:00:00.000+0000,2026-01-10T00:00:00.000+0000,2026-01-08T00:00:00.000+0000,2026-01-10T00:00:00.000+0000\n   1,2026-01-09T00:00:00.000+0000,2026-01-09T00:00:00.000+0000,2026-01-09T00:00:00.000+0000,2026-01-09T00:00:01.000+0000\n   1,2026-01-10T00:00:00.000+0000,1816-03-29T05:56:09.067+0000,2026-01-10T00:00:00.000+0000,1816-03-29T05:56:09.067+0000\n#'\"")
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
