// Copyright 2026 Goldman Sachs
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

package org.finos.legend.engine.plan.execution.stores.relational.ds.specifications;

import org.finos.legend.engine.plan.execution.stores.relational.connection.authentication.AuthenticationStrategy;
import org.finos.legend.engine.plan.execution.stores.relational.connection.driver.DatabaseManager;
import org.finos.legend.engine.plan.execution.stores.relational.connection.ds.DataSourceSpecification;
import org.finos.legend.engine.plan.execution.stores.relational.connection.ds.state.IdentityState;
import org.finos.legend.engine.plan.execution.stores.relational.ds.specifications.keys.LocalDuckDBDataSourceSpecificationKey;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * DataSourceSpecification for local in-memory DuckDB test connections.
 * Similar to LocalH2DataSourceSpecification, it runs setup SQLs when a
 * connection is first obtained from the pool to populate test data.
 */
public class LocalDuckDBDataSourceSpecification extends DataSourceSpecification
{
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(LocalDuckDBDataSourceSpecification.class);

    private static final int MAX_POOL_SIZE = 100;
    private static final int MIN_POOL_SIZE = 0;

    public LocalDuckDBDataSourceSpecification(LocalDuckDBDataSourceSpecificationKey key, DatabaseManager databaseManager, AuthenticationStrategy authenticationStrategy)
    {
        super(key, databaseManager, authenticationStrategy, new Properties(), MAX_POOL_SIZE, MIN_POOL_SIZE);
        this.extraDatasourceProperties.put(DuckDBDataSourceSpecification.DUCKDB_PATH, "");
    }

    @Override
    protected Connection getConnection(IdentityState identityState, Supplier<DataSource> dataSourceBuilder)
    {
        Connection connection = super.getConnection(identityState, dataSourceBuilder);
        LocalDuckDBDataSourceSpecificationKey key = (LocalDuckDBDataSourceSpecificationKey) this.datasourceKey;

        if (key.getTestDataSetupSqls() != null && !key.getTestDataSetupSqls().isEmpty())
        {
            try
            {
                LOGGER.debug("Executing DuckDB test data setup SQLs");
                for (String sql : key.getTestDataSetupSqls())
                {
                    try (Statement statement = connection.createStatement())
                    {
                        statement.executeUpdate(sql);
                    }
                }
            }
            catch (SQLException e)
            {
                throw new RuntimeException("Failed to execute DuckDB test data setup SQLs", e);
            }
        }
        return connection;
    }
}

