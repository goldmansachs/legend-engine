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

import org.finos.legend.engine.plan.execution.stores.relational.connection.ds.DataSourceSpecificationKey;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * DataSourceSpecificationKey for local in-memory DuckDB test connections.
 * Includes test data setup SQLs in equals/hashCode to ensure different test
 * suites get different connection pools, providing concurrency isolation.
 */
public class LocalDuckDBDataSourceSpecificationKey implements DataSourceSpecificationKey
{
    private final List<String> testDataSetupSqls;
    private final long setupCheckSum;

    public LocalDuckDBDataSourceSpecificationKey(List<String> testDataSetupSqls)
    {
        this.testDataSetupSqls = testDataSetupSqls;
        Checksum crc32 = new CRC32();
        byte[] bytes = this.testDataSetupSqls.stream().collect(Collectors.joining(";")).getBytes();
        crc32.update(bytes, 0, bytes.length);
        this.setupCheckSum = crc32.getValue();
    }

    public List<String> getTestDataSetupSqls()
    {
        return this.testDataSetupSqls;
    }

    @Override
    public String shortId()
    {
        return "LocalDuckDB_" +
                "sqlCS:" + setupCheckSum;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        LocalDuckDBDataSourceSpecificationKey that = (LocalDuckDBDataSourceSpecificationKey) o;
        return Objects.equals(testDataSetupSqls, that.testDataSetupSqls);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(testDataSetupSqls);
    }

    @Override
    public String toString()
    {
        return "LocalDuckDBDataSourceSpecificationKey{" +
                "setupCheckSum=" + setupCheckSum +
                '}';
    }
}

