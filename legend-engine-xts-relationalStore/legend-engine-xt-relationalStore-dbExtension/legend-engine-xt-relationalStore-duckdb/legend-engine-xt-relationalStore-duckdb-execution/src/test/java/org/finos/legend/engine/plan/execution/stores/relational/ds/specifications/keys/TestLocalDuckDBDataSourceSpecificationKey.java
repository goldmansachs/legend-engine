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

package org.finos.legend.engine.plan.execution.stores.relational.ds.specifications.keys;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for LocalDuckDBDataSourceSpecificationKey, ensuring that keys with the
 * same setup SQLs are equal (for connection pool sharing) and keys with different
 * setup SQLs are not (for concurrency isolation).
 */
public class TestLocalDuckDBDataSourceSpecificationKey
{
    @Test
    public void testEqualKeysWithSameSqls()
    {
        List<String> sqls = Arrays.asList("CREATE TABLE t1 AS SELECT * FROM read_csv('/tmp/a.csv');", "CREATE TABLE t2 AS SELECT * FROM read_csv('/tmp/b.csv');");
        LocalDuckDBDataSourceSpecificationKey key1 = new LocalDuckDBDataSourceSpecificationKey(sqls);
        LocalDuckDBDataSourceSpecificationKey key2 = new LocalDuckDBDataSourceSpecificationKey(sqls);

        Assert.assertEquals(key1, key2);
        Assert.assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void testDifferentKeysWithDifferentSqls()
    {
        LocalDuckDBDataSourceSpecificationKey key1 = new LocalDuckDBDataSourceSpecificationKey(
                Arrays.asList("CREATE TABLE t1 AS SELECT * FROM read_csv('/tmp/a.csv');"));
        LocalDuckDBDataSourceSpecificationKey key2 = new LocalDuckDBDataSourceSpecificationKey(
                Arrays.asList("CREATE TABLE t2 AS SELECT * FROM read_csv('/tmp/b.csv');"));

        Assert.assertNotEquals(key1, key2);
    }

    @Test
    public void testShortIdContainsDuckDB()
    {
        LocalDuckDBDataSourceSpecificationKey key = new LocalDuckDBDataSourceSpecificationKey(
                Collections.singletonList("CREATE TABLE t1 AS SELECT 1;"));
        Assert.assertTrue(key.shortId().startsWith("LocalDuckDB_"));
    }

    @Test
    public void testEmptySqls()
    {
        LocalDuckDBDataSourceSpecificationKey key1 = new LocalDuckDBDataSourceSpecificationKey(Collections.emptyList());
        LocalDuckDBDataSourceSpecificationKey key2 = new LocalDuckDBDataSourceSpecificationKey(Collections.emptyList());

        Assert.assertEquals(key1, key2);
        Assert.assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void testGetTestDataSetupSqls()
    {
        List<String> sqls = Arrays.asList("SQL1;", "SQL2;");
        LocalDuckDBDataSourceSpecificationKey key = new LocalDuckDBDataSourceSpecificationKey(sqls);

        Assert.assertEquals(sqls, key.getTestDataSetupSqls());
    }

    @Test
    public void testNotEqualToNull()
    {
        LocalDuckDBDataSourceSpecificationKey key = new LocalDuckDBDataSourceSpecificationKey(Collections.singletonList("SQL;"));
        Assert.assertNotEquals(key, null);
    }

    @Test
    public void testNotEqualToDifferentType()
    {
        LocalDuckDBDataSourceSpecificationKey key = new LocalDuckDBDataSourceSpecificationKey(Collections.singletonList("SQL;"));
        Assert.assertNotEquals(key, "not a key");
    }

    @Test
    public void testSameInstanceEqual()
    {
        LocalDuckDBDataSourceSpecificationKey key = new LocalDuckDBDataSourceSpecificationKey(Collections.singletonList("SQL;"));
        Assert.assertEquals(key, key);
    }
}

