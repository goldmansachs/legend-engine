// Copyright 2024 Goldman Sachs
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

package org.finos.legend.engine.changetoken.generation;

import org.junit.Test;

public class GenerateDiffTest extends GenerateDiffTestBase
{
    @Test
    public void testAddField()
    {
        expect(diff("###Pure\n" +
                                "Class meta::pure::changetoken::tests::SampleClass\n" +
                                "{\n" +
                                "  name: String[1] = 'London';  \n" +
                                "  location: model::domain::referenceData::geography::GeographicCoordinate[1] = ^model::domain::referenceData::geography::GeographicCoordinate(latitude = 51.507356, longitude = -0.127706);\n" +
                                "}\n" +
                                "\n" +
                                "function model::domain::referenceData::geography::NullGeographicCoordinate(): model::domain::referenceData::geography::GeographicCoordinate[1]\n" +
                                "{\n" +
                                "  ^model::domain::referenceData::geography::GeographicCoordinate(latitude=0.0, longitude=0.0)\n" +
                                "}\n",
                        "###Pure\n" +
                                "Class model::domain::referenceData::geography::GeographicCoordinate\n" +
                                "{\n" +
                                "  latitude: Float[1];\n" +
                                "  longitude: Float[1];\n" +
                                "}",
                        "",
                        "",
                        "{\n" +
                                "  \"@type\": \"meta::pure::changetoken::Versions\",\n" +
                                "  \"versions\": [\n" +
                                "    {\n" +
                                "      \"@type\": \"meta::pure::changetoken::Version\",\n" +
                                "      \"version\": \"da39a3ee5e6b4b0d3255bfef95601890afd80709\"\n" +
                                "    }\n" +
                                "  ]\n" +
                                "}\n"),
                "{\n" +
                        "  \"@type\": \"meta::pure::changetoken::Versions\",\n" +
                        "  \"versions\": [\n" +
                        "    {\n" +
                        "      \"@type\": \"meta::pure::changetoken::Version\",\n" +
                        "      \"version\": \"da39a3ee5e6b4b0d3255bfef95601890afd80709\"\n" +
                        "    },\n" +
                        "    {\n" +
                        "      \"@type\": \"meta::pure::changetoken::Version\",\n" +
                        "      \"version\": \"d0a63683d2068d0df3da998d81717fe45ac5d459\",\n" +
                        "      \"prevVersion\": \"da39a3ee5e6b4b0d3255bfef95601890afd80709\",\n" +
                        "      \"changeTokens\": [\n" +
                        "        {\n" +
                        "          \"@type\": \"meta::pure::changetoken::AddedClass\",\n" +
                        "          \"class\": \"meta::pure::changetoken::tests::SampleClass\"\n" +
                        "        },\n" +
                        "        {\n" +
                        "          \"@type\": \"meta::pure::changetoken::AddField\",\n" +
                        "          \"fieldName\": \"location\",\n" +
                        "          \"fieldType\": \"model::domain::referenceData::geography::GeographicCoordinate[1]\",\n" +
                        "          \"defaultValue\": {\n" +
                        "            \"@type\": \"meta::pure::changetoken::ConstValue\",\n" +
                        "            \"value\": {\n" +
                        "              \"@type\": \"model::domain::referenceData::geography::GeographicCoordinate\",\n" +
                        "              \"latitude\": 51.507356,\n" +
                        "              \"longitude\": -0.127706\n" +
                        "            }\n" +
                        "          },\n" +
                        "          \"safeCast\": true,\n" +
                        "          \"class\": \"meta::pure::changetoken::tests::SampleClass\"\n" +
                        "        },\n" +
                        "        {\n" +
                        "          \"@type\": \"meta::pure::changetoken::AddField\",\n" +
                        "          \"fieldName\": \"name\",\n" +
                        "          \"fieldType\": \"String[1]\",\n" +
                        "          \"defaultValue\": {\n" +
                        "            \"@type\": \"meta::pure::changetoken::ConstValue\",\n" +
                        "            \"value\": \"London\"\n" +
                        "          },\n" +
                        "          \"safeCast\": true,\n" +
                        "          \"class\": \"meta::pure::changetoken::tests::SampleClass\"\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}\n");
    }
}
