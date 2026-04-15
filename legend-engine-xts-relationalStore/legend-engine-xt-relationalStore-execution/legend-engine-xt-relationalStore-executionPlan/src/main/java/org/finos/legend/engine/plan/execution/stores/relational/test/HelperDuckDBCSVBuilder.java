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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HelperDuckDBCSVBuilder
{
    private final RelationalCSVData relationalData;

    public HelperDuckDBCSVBuilder(RelationalCSVData relationalData)
    {
        this.relationalData = relationalData;
    }

    public List<String> buildSqls()
    {
        if (relationalData == null || relationalData.tables == null || relationalData.tables.isEmpty())
        {
            return Collections.emptyList();
        }

        List<String> sqls = new ArrayList<>();
        for (RelationalCSVTable table : relationalData.tables)
        {
            sqls.addAll(buildTableSqls(table));
        }
        return sqls;
    }

    private static List<String> buildTableSqls(RelationalCSVTable table)
    {
        String schemaName = resolveSchemaName(table.schema);
        String tableName = table.table != null ? table.table : "";
        String qualifiedName = schemaName.equals("main") ? tableName : schemaName + "." + tableName;

        List<String> sqls = new ArrayList<>();

        if (!schemaName.equals("main"))
        {
            sqls.add("CREATE SCHEMA IF NOT EXISTS " + schemaName + ";");
        }

        sqls.add("DROP TABLE IF EXISTS " + qualifiedName + ";");

        if (table.values == null || table.values.isEmpty())
        {
            return sqls;
        }

        String[] lines = table.values.split("\n", -1);
        if (lines.length == 0 || lines[0].isEmpty())
        {
            return sqls;
        }

        List<String> columns = parseCsvLine(lines[0]);

        List<RelationalCSVTableColumnType> columnTypes = table.columnTypes != null ? table.columnTypes : Collections.emptyList();
        String columnsDef = columns.stream()
                .map(col ->
                {
                    String type = columnTypes.stream()
                            .filter(ct -> ct.name.equals(col))
                            .map(ct -> ct.type)
                            .findFirst()
                            .orElse("VARCHAR");
                    return "\"" + col + "\" " + type;
                })
                .collect(Collectors.joining(", "));
        sqls.add("CREATE TABLE IF NOT EXISTS " + qualifiedName + " (" + columnsDef + ");");

        for (int i = 1; i < lines.length; i++)
        {
            String line = lines[i];
            if (line.isEmpty())
            {
                continue;
            }
            List<String> values = parseCsvLine(line);
            String insertSql = "INSERT INTO " + qualifiedName + " VALUES ("
                    + values.stream()
                            .map(v -> "'" + v.replace("'", "''") + "'")
                            .collect(Collectors.joining(", "))
                    + ");";
            sqls.add(insertSql);
        }

        return sqls;
    }

    static List<String> parseCsvLine(String line)
    {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++)
        {
            char c = line.charAt(i);
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"')
                    {
                        current.append('"');
                        i++;
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    current.append(c);
                }
            }
            else
            {
                if (c == '"')
                {
                    inQuotes = true;
                }
                else if (c == ',')
                {
                    fields.add(current.toString());
                    current.setLength(0);
                }
                else
                {
                    current.append(c);
                }
            }
        }
        fields.add(current.toString());
        return fields;
    }

    private static String resolveSchemaName(String schema)
    {
        return (schema == null || schema.isEmpty() || schema.equals("default")) ? "main" : schema;
    }
}

