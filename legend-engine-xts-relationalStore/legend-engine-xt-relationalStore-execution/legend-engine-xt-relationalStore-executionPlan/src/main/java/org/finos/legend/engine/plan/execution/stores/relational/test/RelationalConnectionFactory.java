//  Copyright 2022 Goldman Sachs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package org.finos.legend.engine.plan.execution.stores.relational.test;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.protocol.pure.v1.extension.ConnectionFactoryExtension;
import org.finos.legend.engine.protocol.pure.v1.extension.TestConnectionBuildParameters;
import org.finos.legend.engine.protocol.pure.v1.model.data.EmbeddedData;
import org.finos.legend.engine.protocol.pure.v1.model.data.relation.RelationElementsData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.connection.Connection;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.data.DataElement;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.mappingTest.InputData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.Store;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.DatabaseType;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.RelationalDatabaseConnection;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.authentication.TestDatabaseAuthenticationStrategy;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.specification.DuckDBDatasourceSpecification;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.specification.LocalH2DatasourceSpecification;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.data.RelationalCSVData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.data.RelationalCSVTable;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.data.RelationalCSVTableColumnType;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.mapping.mappingTest.RelationalInputData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.mapping.mappingTest.RelationalInputType;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Column;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Database;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Schema;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Table;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.BigInt;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Binary;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Bit;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Char;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Date;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Decimal;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Json;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Numeric;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Real;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.SemiStructured;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.SmallInt;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Timestamp;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.TinyInt;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.VarChar;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Varbinary;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RelationalConnectionFactory implements ConnectionFactoryExtension
{
    @Override
    public MutableList<String> group()
    {
        return org.eclipse.collections.impl.factory.Lists.mutable.with("Store", "Relational", "-Core");
    }

    @Override
    public Optional<Connection> tryBuildFromInputData(InputData inputData)
    {
        if (inputData instanceof RelationalInputData)
        {
            RelationalInputData relationalInputData = (RelationalInputData) inputData;
            RelationalDatabaseConnection connection = new RelationalDatabaseConnection();
            connection.databaseType = DatabaseType.H2;
            connection.type = DatabaseType.H2;
            connection.element = relationalInputData.database;
            connection.authenticationStrategy = new TestDatabaseAuthenticationStrategy();
            LocalH2DatasourceSpecification localH2DatasourceSpecification = new LocalH2DatasourceSpecification();
            if (relationalInputData.inputType == RelationalInputType.SQL)
            {
                localH2DatasourceSpecification.testDataSetupSqls = Lists.mutable.of(relationalInputData.data.split("(?<!\\\\);")).collect(r -> r.replace("\\;", ";") + ";");
            }
            else if (relationalInputData.inputType == RelationalInputType.CSV)
            {
                localH2DatasourceSpecification.testDataSetupCsv = relationalInputData.data;
            }
            else
            {
                throw new RuntimeException(relationalInputData.inputType + " is not supported");
            }
            connection.datasourceSpecification = localH2DatasourceSpecification;
            return Optional.of(connection);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Connection> tryBuildFromConnection(Connection connection, String testData, String element)
    {
        return ConnectionFactoryExtension.super.tryBuildFromConnection(connection, testData, element);
    }

    @Override
    public Optional<Pair<Connection, List<Closeable>>> tryBuildTestConnection(Connection sourceConnection, List<EmbeddedData> data)
    {
        return tryBuildTestConnection(sourceConnection, data, TestConnectionBuildParameters.NONE);
    }

    @Override
    public Optional<Pair<Connection, List<Closeable>>> tryBuildTestConnection(Connection sourceConnection, List<EmbeddedData> data, TestConnectionBuildParameters hints)
    {
        List<RelationalCSVData> relationalCSVDataList = ListIterate.selectInstancesOf(data, RelationalCSVData.class);
        if (data.size() == relationalCSVDataList.size() && sourceConnection instanceof RelationalDatabaseConnection && !data.isEmpty())
        {
            RelationalCSVData relationalData;
            if (relationalCSVDataList.size() == 1)
            {
                relationalData = relationalCSVDataList.get(0);
            }
            else
            {
                relationalData = new RelationalCSVData();
                relationalData.tables = ListIterate.flatCollect(relationalCSVDataList, a -> a.tables);
            }
            if (hints.isRelation() && hints.getPureModelContextData() != null)
            {
                Database database = (Database) ListIterate.detect(
                        hints.getPureModelContextData().getElementsOfType(org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.Store.class),
                        s -> s instanceof Database && s.getPath().equals(sourceConnection.element));
                if (database != null)
                {
                    enrichColumnTypes(database, relationalData);
                }
            }
            return this.buildRelationalTestConnection(sourceConnection.element, relationalData, hints);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Pair<Connection, List<Closeable>>> tryBuildConnectionForStoreData(Map<String, DataElement> dataElements, Map<Store, EmbeddedData> storeTestData)
    {
        return tryBuildConnectionForStoreData(dataElements, storeTestData, TestConnectionBuildParameters.NONE);
    }

    @Override
    public Optional<Pair<Connection, List<Closeable>>> tryBuildConnectionForStoreData(Map<String, DataElement> dataElements, Map<Store, EmbeddedData> storeTestData, TestConnectionBuildParameters hints)
    {
        if (storeTestData == null || storeTestData.isEmpty())
        {
            return Optional.empty();
        }
        if (storeTestData.size() == 1)
        {
            Store store = storeTestData.keySet().stream().findFirst().get();
            EmbeddedData embeddedData = storeTestData.values().stream().findFirst().get();
            if (embeddedData instanceof RelationElementsData)
            {
                embeddedData = buildRelationCSVDataFromRelationElementData(Collections.singletonList((RelationElementsData) embeddedData));
            }
            return tryBuildTestConnectionsForStore(dataElements, store, embeddedData, hints);
        }
        List<EmbeddedData> embeddedData = new ArrayList<>(storeTestData.values());
        boolean isRelational = storeTestData.keySet().stream().allMatch(db -> db instanceof Database) && embeddedData.stream().allMatch(rD -> rD instanceof RelationalCSVData || rD instanceof RelationElementsData);
        if (isRelational)
        {
            List<RelationalCSVData> relationalCSVDataList = ListIterate.selectInstancesOf(embeddedData, RelationalCSVData.class);
            List<RelationElementsData> relationElementsDataList = ListIterate.selectInstancesOf(embeddedData, RelationElementsData.class);
            if (!relationElementsDataList.isEmpty())
            {
                relationalCSVDataList.add(buildRelationCSVDataFromRelationElementData(relationElementsDataList));
            }
            RelationalCSVData relationalData = new RelationalCSVData();
            relationalData.tables = ListIterate.flatCollect(relationalCSVDataList, a -> a.tables);
            return this.buildRelationalTestConnection(null, relationalData, hints);
        }
        return Optional.empty();
    }

    private RelationalCSVData buildRelationCSVDataFromRelationElementData(List<RelationElementsData> relationElementsData)
    {
        RelationalCSVData relationalCSVData = new RelationalCSVData();
        relationalCSVData.tables = ListIterate.flatCollect(relationElementsData, data ->
                data.relationElements.stream().map(relationElement ->
                {
                    RelationalCSVTable table = new RelationalCSVTable();
                    table.schema = relationElement.paths.get(0);
                    table.table = relationElement.paths.get(1);
                    table.values = String.join(",", relationElement.columns) + "\n" + relationElement.rows.stream().map(row -> String.join(",", row.values)).collect(Collectors.joining("\n"));
                    return table;
                }).collect(Collectors.toList()));
        return relationalCSVData;
    }

    protected Optional<Pair<Connection, List<Closeable>>> buildRelationalTestConnection(String element, RelationalCSVData data, TestConnectionBuildParameters hints)
    {
        return hints.isRelation() ? buildDuckDBTestConnection(element, data) : buildH2TestConnection(element, data);
    }

    protected Optional<Pair<Connection, List<Closeable>>> buildH2TestConnection(String element, RelationalCSVData data)
    {
        RelationalDatabaseConnection connection = new RelationalDatabaseConnection();
        connection.databaseType = DatabaseType.H2;
        connection.type = DatabaseType.H2;
        connection.element = element;
        connection.authenticationStrategy = new TestDatabaseAuthenticationStrategy();
        LocalH2DatasourceSpecification localH2DatasourceSpecification = new LocalH2DatasourceSpecification();
        localH2DatasourceSpecification.testDataSetupCsv = new HelperRelationalCSVBuilder(data).build();
        connection.datasourceSpecification = localH2DatasourceSpecification;
        return Optional.of(Tuples.pair(connection, Collections.emptyList()));
    }

    protected Optional<Pair<Connection, List<Closeable>>> buildDuckDBTestConnection(String element, RelationalCSVData data)
    {
        List<String> setupSqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        RelationalDatabaseConnection connection = new RelationalDatabaseConnection();
        connection.databaseType = DatabaseType.DuckDB;
        connection.type = DatabaseType.DuckDB;
        connection.element = element;
        connection.authenticationStrategy = new TestDatabaseAuthenticationStrategy();

        DuckDBDatasourceSpecification duckDBSpec = new DuckDBDatasourceSpecification();
        duckDBSpec.path = "";
        duckDBSpec.testDataSetupSqls = setupSqls;
        connection.datasourceSpecification = duckDBSpec;

        return Optional.of(Tuples.pair(connection, Collections.emptyList()));
    }

    @Override
    public Optional<Pair<Connection, List<Closeable>>> tryBuildTestConnectionsForStore(Map<String, DataElement> dataElements, Store testStore, EmbeddedData data)
    {
        return tryBuildTestConnectionsForStore(dataElements, testStore, data, TestConnectionBuildParameters.NONE);
    }

    @Override
    public Optional<Pair<Connection, List<Closeable>>> tryBuildTestConnectionsForStore(Map<String, DataElement> dataElements, Store testStore, EmbeddedData data, TestConnectionBuildParameters hints)
    {
        if (testStore instanceof Database)
        {
            if (data instanceof RelationalCSVData)
            {
                enrichColumnTypes((Database) testStore, (RelationalCSVData) data);
            }
            RelationalDatabaseConnection connection = new RelationalDatabaseConnection();
            connection.element = testStore.getPath();
            return this.tryBuildTestConnection(connection, Lists.mutable.of(data), hints);
        }
        return Optional.empty();
    }

    static void enrichColumnTypes(Database database, RelationalCSVData data)
    {
        if (data.tables == null)
        {
            return;
        }
        Map<String, Table> tableIndex = buildTableIndex(database);
        for (RelationalCSVTable csvTable : data.tables)
        {
            if (csvTable.columnTypes != null && !csvTable.columnTypes.isEmpty())
            {
                continue;
            }
            Table dbTable = tableIndex.get(csvTable.schema + "." + csvTable.table);
            if (dbTable == null || dbTable.columns == null)
            {
                continue;
            }
            List<RelationalCSVTableColumnType> columnTypes = new ArrayList<>();
            for (Column col : dbTable.columns)
            {
                if (col.name != null && col.type != null)
                {
                    columnTypes.add(new RelationalCSVTableColumnType(col.name, dataTypeToSqlTextDuckDB(col.type)));
                }
            }
            if (!columnTypes.isEmpty())
            {
                csvTable.columnTypes = columnTypes;
            }
        }
    }

    private static Map<String, Table> buildTableIndex(Database database)
    {
        Map<String, Table> index = new java.util.HashMap<>();
        if (database.schemas != null)
        {
            for (Schema schema : database.schemas)
            {
                if (schema.tables != null)
                {
                    for (Table table : schema.tables)
                    {
                        index.put(schema.name + "." + table.name, table);
                    }
                }
            }
        }
        return index;
    }

    static String dataTypeToSqlTextDuckDB(org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.DataType dataType)
    {
        if (dataType instanceof VarChar)
        {
            return "VARCHAR(" + ((VarChar) dataType).size + ")";
        }
        else if (dataType instanceof Char)
        {
            return "CHAR(" + ((Char) dataType).size + ")";
        }
        else if (dataType instanceof org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Integer)
        {
            return "INT";
        }
        else if (dataType instanceof BigInt)
        {
            return "BIGINT";
        }
        else if (dataType instanceof SmallInt)
        {
            return "SMALLINT";
        }
        else if (dataType instanceof TinyInt)
        {
            return "TINYINT";
        }
        else if (dataType instanceof org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Float)
        {
            return "FLOAT";
        }
        else if (dataType instanceof org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.datatype.Double)
        {
            return "DOUBLE";
        }
        else if (dataType instanceof Real)
        {
            return "REAL";
        }
        else if (dataType instanceof Decimal)
        {
            Decimal d = (Decimal) dataType;
            return "DECIMAL(" + d.precision + ", " + d.scale + ")";
        }
        else if (dataType instanceof Numeric)
        {
            Numeric n = (Numeric) dataType;
            return "NUMERIC(" + n.precision + ", " + n.scale + ")";
        }
        else if (dataType instanceof Date)
        {
            return "DATE";
        }
        else if (dataType instanceof Timestamp)
        {
            return "TIMESTAMP";
        }
        else if (dataType instanceof Bit)
        {
            return "BOOL";
        }
        else if (dataType instanceof Binary)
        {
            return "BINARY(" + ((Binary) dataType).size + ")";
        }
        else if (dataType instanceof Varbinary)
        {
            return "VARBINARY(" + ((Varbinary) dataType).size + ")";
        }
        else if (dataType instanceof Json)
        {
            return "JSON";
        }
        else if (dataType instanceof SemiStructured)
        {
            return "JSON";
        }
        return "VARCHAR";
    }
}
