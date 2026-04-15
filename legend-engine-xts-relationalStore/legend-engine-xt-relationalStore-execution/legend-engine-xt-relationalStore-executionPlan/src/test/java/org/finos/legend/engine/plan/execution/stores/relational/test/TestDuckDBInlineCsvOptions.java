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

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Probes what DuckDB 1.3 actually supports for loading inline CSV without a persistent file.
 * Run with: mvn test -pl ..executionPlan -Dtest=TestDuckDBInlineCsvOptions
 */
public class TestDuckDBInlineCsvOptions
{
    @Test
    public void probeDuckDBCsvOptions() throws Exception
    {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"))
        {
            // Option A: plain string literal (known to fail - treated as file path)
            try (Statement s = conn.createStatement())
            {
                s.execute("CREATE TABLE optA AS SELECT * FROM read_csv('id,name\n1,Alice\n', header=true)");
                System.out.println("OPTION A (plain literal): PASS");
            }
            catch (Exception e)
            {
                System.out.println("OPTION A (plain literal): FAIL - " + e.getMessage());
            }

            // Option B: subquery wrapper (known to fail - binder error)
            try (Statement s = conn.createStatement())
            {
                s.execute("CREATE TABLE optB AS SELECT * FROM read_csv((SELECT $$id,name\n1,Alice\n$$), header=true)");
                System.out.println("OPTION B (SELECT subquery): PASS");
            }
            catch (Exception e)
            {
                System.out.println("OPTION B (SELECT subquery): FAIL - " + e.getMessage());
            }

            // Option C: temp file on /tmp (standard tmpfs on Linux)
            Path tmp = Files.createTempFile("duckdb_test_", ".csv");
            try
            {
                Files.write(tmp, "id,name\n1,Alice\n2,Bob\n".getBytes());
                try (Statement s = conn.createStatement())
                {
                    s.execute("CREATE TABLE optC AS SELECT * FROM read_csv('" + tmp.toAbsolutePath() + "', header=true, columns={id:'INT', name:'VARCHAR'})");
                    ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM optC");
                    rs.next();
                    System.out.println("OPTION C (tmp file): PASS - rows=" + rs.getInt(1));
                    Assert.assertEquals(2, rs.getInt(1));
                }
            }
            finally
            {
                Files.deleteIfExists(tmp);
            }

            // Option D: DuckDB APPENDER via JDBC setObject (not SQL-level, requires DuckDB API)
            // skipped - requires DuckDB-specific Appender class

            // Option E: INSERT with VALUES (our old approach - requires parsing CSV in Java)
            try (Statement s = conn.createStatement())
            {
                s.execute("CREATE TABLE optE(id INT, name VARCHAR)");
                s.execute("INSERT INTO optE VALUES (1, 'Alice'), (2, 'Bob')");
                ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM optE");
                rs.next();
                System.out.println("OPTION E (INSERT VALUES): PASS - rows=" + rs.getInt(1));
                Assert.assertEquals(2, rs.getInt(1));
            }

            // Option F: DuckDB Appender API (zero-copy, no SQL parsing)
            try
            {
                Class<?> appenderClass = Class.forName("org.duckdb.DuckDBAppender");
                // If we get here, DuckDB Appender is available
                System.out.println("OPTION F (DuckDB Appender): AVAILABLE");
            }
            catch (ClassNotFoundException e)
            {
                System.out.println("OPTION F (DuckDB Appender): NOT AVAILABLE - " + e.getMessage());
            }
        }
    }
}

