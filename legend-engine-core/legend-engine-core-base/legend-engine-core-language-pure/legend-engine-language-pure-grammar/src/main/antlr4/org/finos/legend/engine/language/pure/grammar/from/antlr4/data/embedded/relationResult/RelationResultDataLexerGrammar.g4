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

lexer grammar RelationResultDataLexerGrammar;

// No import — this is a standalone lexer for flat tabular data (columns + rows)
// without paths or table names. Used for assertion relation results.

// In default mode, we immediately lex table content (no need for TABLE_START trigger).
// The text received is the island content between #{ and }#, e.g.:
//   id, firstName, lastName
//   1, John, Smith
//   2, Jane, Doe

ROW_VALUE:      (EscSeq | ~[,\r\n])+;
ROW_COMMA:      ',';
NEWLINE:        '\r'?'\n' [ \t]*;

fragment EscSeq: '\\' .;

