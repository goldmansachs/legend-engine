// Copyright 2020 Goldman Sachs
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

package org.finos.legend.engine.language.pure.compiler.toPureGraph;

import org.finos.legend.engine.language.pure.compiler.test.TestCompilationFromGrammar;
import org.junit.Test;

public class TestDataQualityCompilationFromGrammar extends TestCompilationFromGrammar.TestCompilationFromGrammarTestSuite
{
    @Override
    public String getDuplicatedElementTestCode()
    {
        return COMPILATION_PREREQUISITE_CODE +
                "###DataQualityValidation\n" +
                "DataQualityValidation meta::dataquality::Person\n" + // duplicated the validation name, same name as the class
                "{\n" +
                "    context: fromMappingAndRuntime(meta::dataquality::dataqualitymappings, meta::dataquality::DataQualityRuntime);\n" +
                "    filter: p:meta::dataquality::Person[1] | $p.name=='John';\n" +
                "    validationTree: $[\n" +
                "      meta::dataquality::Person<mustBeOfLegalAge>{\n" +
                "        name\n" +
                "      }\n" +
                "    ]$;\n" +
                "}";
    }

    @Override
    public String getDuplicatedElementTestExpectedErrorMessage()
    {
        return "COMPILATION error at [92:1-101:1]: Duplicated element 'meta::dataquality::Person'";
    }

    @Test
    public void testHappyPath()
    {
        TestCompilationFromGrammar.TestCompilationFromGrammarTestSuite.test(COMPILATION_PREREQUISITE_CODE +
                "###DataQualityValidation\n" +
                "DataQualityValidation meta::dataquality::PersonDataQualityValidation\n" +
                "{\n" +
                "    context: fromMappingAndRuntime(meta::dataquality::dataqualitymappings, meta::dataquality::DataQualityRuntime);\n" +
                "    filter: p:meta::dataquality::Person[1] | $p.name=='John';\n" +
                "    validationTree: $[\n" +
                "      meta::dataquality::Person<mustBeOfLegalAge>{\n" +
                "        name\n" +
                "      }\n" +
                "    ]$;\n" +
                "}");
    }

    @Test
    public void testRootClassNoModelConstraints()
    {
        TestCompilationFromGrammar.TestCompilationFromGrammarTestSuite.test(COMPILATION_PREREQUISITE_CODE +
                "###DataQualityValidation\n" +
                "DataQualityValidation meta::dataquality::PersonDataQualityValidation\n" +
                "{\n" +
                "    context: fromMappingAndRuntime(meta::dataquality::dataqualitymappings, meta::dataquality::DataQualityRuntime);\n" +
                "    filter: p:meta::dataquality::Person[1] | $p.name=='John';\n" +
                "    validationTree: $[\n" +
                "      meta::dataquality::Person{\n" +
                "        name\n" +
                "      }\n" +
                "    ]$;\n" +
                "}");
    }

    @Test
    public void testRootClassNoStructuralConstraints()
    {
        TestCompilationFromGrammar.TestCompilationFromGrammarTestSuite.test(COMPILATION_PREREQUISITE_CODE +
                "###DataQualityValidation\n" +
                "DataQualityValidation meta::dataquality::PersonDataQualityValidation\n" +
                "{\n" +
                "    context: fromMappingAndRuntime(meta::dataquality::dataqualitymappings, meta::dataquality::DataQualityRuntime);\n" +
                "    filter: p:meta::dataquality::Person[1] | $p.name=='John';\n" +
                "    validationTree: $[\n" +
                "      meta::dataquality::Person<mustBeOfLegalAge>{\n" +
                "      }\n" +
                "    ]$;\n" +
                "}");
    }






    private static final String COMPILATION_PREREQUISITE_CODE = "###Connection\n" +
            "RelationalDatabaseConnection meta::dataquality::H2\n" +
            "{\n" +
            "  store: meta::dataquality::db;\n" +
            "  type: H2;\n" +
            "  specification: LocalH2\n" +
            "  { \n" +
            "    testDataSetupSqls: [];\n" +
            "  };\n" +
            "  auth: DefaultH2;\n" +
            "}\n" +
            "\n" +
            "###Relational\n" +
            "Database meta::dataquality::db\n" +
            "(\n" +
            "   Table personTable (ID INT PRIMARY KEY, FIRSTNAME VARCHAR(200), LASTNAME VARCHAR(200), AGE INT, ADDRESSID INT, FIRMID INT)\n" +
            "   Table addressTable (ID INT PRIMARY KEY, LOCATIONID INT, TYPE INT)\n" +
            "   Table locationTable (ID INT PRIMARY KEY, STREET VARCHAR(200), LOCALITY VARCHAR(200))\n" +
            "\n" +
            "   Join Address_Person(addressTable.ID = personTable.ADDRESSID)\n" +
            "   Join Address_Location(addressTable.LOCATIONID = locationTable.ID)\n" +
            ")\n" +
            "\n" +
            "\n" +
            "###Mapping\n" +
            "Mapping meta::dataquality::dataqualitymappings\n" +
            "(\n" +
            "\n" +
            "   meta::dataquality::Person : Relational\n" +
            "   {\n" +
            "      name : [meta::dataquality::db]personTable.FIRSTNAME,\n" +
            "      age : [meta::dataquality::db]personTable.AGE,\n" +
            "      addresses : [meta::dataquality::db]@Address_Person\n" +
            "   }\n" +
            "\n" +
            "   meta::dataquality::Address : Relational\n" +
            "   {\n" +
            "      addressId : [meta::dataquality::db]addressTable.ID,\n" +
            "      location : [meta::dataquality::db]@Address_Location\n" +
            "   }\n" +
            "\n" +
            "   meta::dataquality::Location : Relational\n" +
            "   {\n" +
            "      street : [meta::dataquality::db]locationTable.STREET,\n" +
            "      locality : [meta::dataquality::db]locationTable.LOCALITY\n" +
            "   }\n" +
            ")\n" +
            "\n" +
            "###Runtime\n" +
            "Runtime meta::dataquality::DataQualityRuntime\n" +
            "{\n" +
            "  mappings:\n" +
            "  [\n" +
            "    meta::dataquality::dataqualitymappings\n" +
            "  ];\n" +
            "  connections:\n" +
            "  [\n" +
            "    meta::dataquality::db:\n" +
            "    [\n" +
            "      connection_1: meta::dataquality::H2\n" +
            "    ]\n" +
            "  ];\n" +
            "}\n" +
            "\n" +
            "\n" +
            "\n" +
            "###Pure\n" +
            "Class meta::dataquality::Person\n" +
            "[\n" +
            "  mustBeOfLegalAge: $this.age >= 18\n" +
            "]\n" +
            "{\n" +
            "   name : String[1];\n" +
            "   age : Integer[1];\n" +
            "   addresses : meta::dataquality::Address[*];\n" +
            "}\n" +
            "\n" +
            "Class meta::dataquality::Address\n" +
            "{\n" +
            "   location: meta::dataquality::Location[1];\n" +
            "   locationStreet: String[1];\n" +
            "   addressId: String[1];\n" +
            "}\n" +
            "\n" +
            "Class meta::dataquality::Location\n" +
            "{\n" +
            "   street: String[1];\n" +
            "   locality: String[1];\n" +
            "}\n" +
            "\n";

}
