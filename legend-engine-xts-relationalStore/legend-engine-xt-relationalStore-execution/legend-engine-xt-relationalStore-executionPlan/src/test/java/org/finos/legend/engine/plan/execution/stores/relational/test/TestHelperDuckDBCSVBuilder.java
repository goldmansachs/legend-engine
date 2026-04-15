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

package org.finos.legend.engine.plan.execution.stores.relational.test;

import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.data.RelationalCSVData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.data.RelationalCSVTable;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.data.RelationalCSVTableColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestHelperDuckDBCSVBuilder
{
    private static RelationalCSVData singleTable(String schema, String table, String values)
    {
        RelationalCSVTable t = new RelationalCSVTable();
        t.schema = schema;
        t.table = table;
        t.values = values;
        RelationalCSVData data = new RelationalCSVData();
        data.tables = Collections.singletonList(t);
        return data;
    }

    private static long countMatching(List<String> sqls, String fragment)
    {
        return sqls.stream().filter(s -> s.contains(fragment)).count();
    }

    @Test
    public void testBuildSqls_SingleTable()
    {
        RelationalCSVData data = singleTable("default", "PersonTable", "id,name,age\n1,Alice,30\n2,Bob,25\n");

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(4, sqls.size());
        Assert.assertEquals(1, countMatching(sqls, "DROP TABLE IF EXISTS PersonTable"));
        Assert.assertEquals(1, countMatching(sqls, "CREATE TABLE IF NOT EXISTS PersonTable"));
        Assert.assertEquals(2, countMatching(sqls, "INSERT INTO PersonTable VALUES"));
        Assert.assertTrue(sqls.stream().anyMatch(s -> s.contains("\"id\" VARCHAR") && s.contains("\"name\" VARCHAR") && s.contains("\"age\" VARCHAR")));
    }

    @Test
    public void testBuildSqls_MultipleTables()
    {
        RelationalCSVData data = new RelationalCSVData();
        data.tables = new ArrayList<>();

        RelationalCSVTable t1 = new RelationalCSVTable();
        t1.schema = "default";
        t1.table = "PersonTable";
        t1.values = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
        data.tables.add(t1);

        RelationalCSVTable t2 = new RelationalCSVTable();
        t2.schema = "default";
        t2.table = "FirmTable";
        t2.values = "id,name\n10,GS\n20,MS\n";
        data.tables.add(t2);

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(9, sqls.size());
        Assert.assertEquals(1, countMatching(sqls, "DROP TABLE IF EXISTS PersonTable"));
        Assert.assertEquals(1, countMatching(sqls, "CREATE TABLE IF NOT EXISTS PersonTable"));
        Assert.assertEquals(3, countMatching(sqls, "INSERT INTO PersonTable VALUES"));
        Assert.assertEquals(1, countMatching(sqls, "DROP TABLE IF EXISTS FirmTable"));
        Assert.assertEquals(1, countMatching(sqls, "CREATE TABLE IF NOT EXISTS FirmTable"));
        Assert.assertEquals(2, countMatching(sqls, "INSERT INTO FirmTable VALUES"));
    }

    @Test
    public void testBuildSqls_CustomSchema()
    {
        RelationalCSVData data = singleTable("mySchema", "PersonTable", "id,name\n1,Alice\n");

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(4, sqls.size());
        Assert.assertEquals(1, countMatching(sqls, "CREATE SCHEMA IF NOT EXISTS mySchema"));
        Assert.assertEquals(1, countMatching(sqls, "DROP TABLE IF EXISTS mySchema.PersonTable"));
        Assert.assertEquals(1, countMatching(sqls, "CREATE TABLE IF NOT EXISTS mySchema.PersonTable"));
        Assert.assertEquals(1, countMatching(sqls, "INSERT INTO mySchema.PersonTable VALUES"));
    }

    @Test
    public void testBuildSqls_DefaultSchemaUsesMain()
    {
        RelationalCSVData data = singleTable("default", "PersonTable", "id,name\n1,Alice\n");

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(0, countMatching(sqls, "CREATE SCHEMA"));
        Assert.assertEquals(1, countMatching(sqls, "DROP TABLE IF EXISTS PersonTable"));
        Assert.assertEquals(1, countMatching(sqls, "CREATE TABLE IF NOT EXISTS PersonTable"));
    }

    @Test
    public void testBuildSqls_NullData()
    {
        List<String> sqls = new HelperDuckDBCSVBuilder(null).buildSqls();
        Assert.assertTrue("null input must produce no SQLs", sqls.isEmpty());
    }

    @Test
    public void testBuildSqls_EmptyTables()
    {
        RelationalCSVData data = new RelationalCSVData();
        data.tables = Collections.emptyList();

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();
        Assert.assertTrue("empty tables must produce no SQLs", sqls.isEmpty());
    }

    @Test
    public void testBuildSqls_NoValues()
    {
        RelationalCSVData data = singleTable("default", "PersonTable", null);

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(1, sqls.size());
        Assert.assertEquals(1, countMatching(sqls, "DROP TABLE IF EXISTS PersonTable"));
    }

    @Test
    public void testBuildSqls_QuotedCsvValues()
    {
        RelationalCSVData data = singleTable("default", "PersonTable", "id,name\n1,\"Smith, John\"\n");

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(3, sqls.size());
        Assert.assertTrue("quoted comma value must be a single SQL value",
                sqls.stream().anyMatch(s -> s.contains("'Smith, John'")));
    }

    @Test
    public void testBuildSqls_SingleQuoteEscaping()
    {
        RelationalCSVData data = singleTable("default", "PersonTable", "id,name\n1,O'Brien\n");

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(3, sqls.size());
        Assert.assertTrue("single quote must be escaped as ''",
                sqls.stream().anyMatch(s -> s.contains("'O''Brien'")));
    }

    @Test
    public void testBuildSqls_ColumnTypesUsed()
    {
        RelationalCSVTable t = new RelationalCSVTable();
        t.schema = "default";
        t.table = "PersonTable";
        t.values = "id,name,age\n1,Alice,30\n";
        t.columnTypes = Arrays.asList(
                new RelationalCSVTableColumnType("id", "INT"),
                new RelationalCSVTableColumnType("name", "VARCHAR(200)"),
                new RelationalCSVTableColumnType("age", "INT")
        );
        RelationalCSVData data = new RelationalCSVData();
        data.tables = Collections.singletonList(t);

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertEquals(3, sqls.size());
        String createSql = sqls.get(1);
        Assert.assertTrue("id must be INT", createSql.contains("\"id\" INT"));
        Assert.assertTrue("name must be VARCHAR(200)", createSql.contains("\"name\" VARCHAR(200)"));
        Assert.assertTrue("age must be INT", createSql.contains("\"age\" INT"));
        Assert.assertTrue("INSERT must have quoted values",
                sqls.stream().anyMatch(s -> s.contains("INSERT INTO PersonTable VALUES ('1', 'Alice', '30')")));
    }

    @Test
    public void testBuildSqls_ColumnTypesPartial()
    {
        RelationalCSVTable t = new RelationalCSVTable();
        t.schema = "default";
        t.table = "PersonTable";
        t.values = "id,name,age\n1,Alice,30\n";
        t.columnTypes = Collections.singletonList(new RelationalCSVTableColumnType("id", "INT"));
        RelationalCSVData data = new RelationalCSVData();
        data.tables = Collections.singletonList(t);

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        String createSql = sqls.get(1);
        Assert.assertTrue("id must be INT", createSql.contains("\"id\" INT"));
        Assert.assertTrue("name must fall back to VARCHAR", createSql.contains("\"name\" VARCHAR"));
        Assert.assertTrue("age must fall back to VARCHAR", createSql.contains("\"age\" VARCHAR"));
    }

    @Test
    public void testBuildSqls_NullColumnTypesFallBackToVarchar()
    {
        RelationalCSVTable t = new RelationalCSVTable();
        t.schema = "default";
        t.table = "PersonTable";
        t.values = "id,name\n1,Alice\n";
        t.columnTypes = null;
        RelationalCSVData data = new RelationalCSVData();
        data.tables = Collections.singletonList(t);

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertTrue("all columns must default to VARCHAR",
                sqls.get(1).contains("\"id\" VARCHAR") && sqls.get(1).contains("\"name\" VARCHAR"));
    }

    @Test
    public void testBuildSqls_EmptyColumnTypesFallBackToVarchar()
    {
        RelationalCSVTable t = new RelationalCSVTable();
        t.schema = "default";
        t.table = "PersonTable";
        t.values = "id,name\n1,Alice\n";
        t.columnTypes = Collections.emptyList();
        RelationalCSVData data = new RelationalCSVData();
        data.tables = Collections.singletonList(t);

        List<String> sqls = new HelperDuckDBCSVBuilder(data).buildSqls();

        Assert.assertTrue("all columns must default to VARCHAR",
                sqls.get(1).contains("\"id\" VARCHAR") && sqls.get(1).contains("\"name\" VARCHAR"));
    }

    @Test
    public void testParseCsvLine_Simple()
    {
        Assert.assertEquals(Arrays.asList("1", "Alice", "30"), HelperDuckDBCSVBuilder.parseCsvLine("1,Alice,30"));
    }

    @Test
    public void testParseCsvLine_QuotedComma()
    {
        Assert.assertEquals(Arrays.asList("1", "Smith, John", "30"), HelperDuckDBCSVBuilder.parseCsvLine("1,\"Smith, John\",30"));
    }

    @Test
    public void testParseCsvLine_EscapedDoubleQuote()
    {
        Assert.assertEquals(Arrays.asList("1", "O\"Brien", "30"), HelperDuckDBCSVBuilder.parseCsvLine("1,\"O\"\"Brien\",30"));
    }

    @Test
    public void testParseCsvLine_SingleQuotePassthrough()
    {
        Assert.assertEquals(Arrays.asList("1", "O'Brien", "30"), HelperDuckDBCSVBuilder.parseCsvLine("1,O'Brien,30"));
    }
}

