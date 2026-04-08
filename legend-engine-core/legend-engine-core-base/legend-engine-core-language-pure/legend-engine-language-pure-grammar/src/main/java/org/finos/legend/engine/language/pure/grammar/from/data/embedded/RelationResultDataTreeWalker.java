// Copyright 2025 Goldman Sachs
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

package org.finos.legend.engine.language.pure.grammar.from.data.embedded;

import org.apache.commons.text.StringEscapeUtils;
import org.finos.legend.engine.language.pure.grammar.from.ParseTreeWalkerSourceInformation;
import org.finos.legend.engine.language.pure.grammar.from.antlr4.data.embedded.relationResult.RelationResultDataParserGrammar;
import org.finos.legend.engine.protocol.pure.m3.SourceInformation;
import org.finos.legend.engine.protocol.pure.v1.model.data.relation.RelationElement;
import org.finos.legend.engine.protocol.pure.v1.model.data.relation.RelationRowTestData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

public class RelationResultDataTreeWalker
{
    private final SourceInformation sourceInformation;

    public RelationResultDataTreeWalker(ParseTreeWalkerSourceInformation walkerSourceInformation, SourceInformation sourceInformation)
    {
        this.sourceInformation = sourceInformation;
    }

    public RelationElement visit(RelationResultDataParserGrammar.DefinitionContext ctx)
    {
        RelationElement element = new RelationElement();
        element.sourceInformation = this.sourceInformation;
        element.paths = Collections.emptyList();

        element.columns = ctx.columnNames().cell().stream()
                .map(cellContext -> cellContext.ROW_VALUE() != null ? StringEscapeUtils.unescapeJava(cellContext.ROW_VALUE().getText().trim()) : "")
                .collect(Collectors.toList());

        element.rows = new ArrayList<>();
        ctx.rows().rowValues().forEach(rowValuesContext ->
        {
            RelationRowTestData row = new RelationRowTestData();
            row.values = rowValuesContext.cell().stream()
                    .map(cellContext -> cellContext.ROW_VALUE() != null ? StringEscapeUtils.unescapeJava(cellContext.ROW_VALUE().getText().trim()) : "")
                    .collect(Collectors.toList());
            // Skip empty rows (all values are empty strings) caused by trailing newlines
            if (row.values.stream().anyMatch(v -> !v.isEmpty()))
            {
                element.rows.add(row);
            }
        });

        return element;
    }
}

