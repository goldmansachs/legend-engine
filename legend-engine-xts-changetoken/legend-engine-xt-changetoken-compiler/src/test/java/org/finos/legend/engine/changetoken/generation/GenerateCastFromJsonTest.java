//  Copyright 2023 Goldman Sachs
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

package org.finos.legend.engine.changetoken.generation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.pure.generated.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.assertThrows;

public class GenerateCastFromJsonTest extends GenerateCastTestBase
{
    @BeforeClass
    public static void setupSuite() throws IOException, ClassNotFoundException
    {
        setupSuiteFromJson("{\n" +
                "  \"@type\": \"meta::pure::changetoken::Versions\",\n" +
                "  \"versions\": [\n" +
                "    {\n" +
                "      \"@type\": \"meta::pure::changetoken::Version\",\n" +
                "      \"version\": \"ftdm:abcdefg123\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"@type\": \"meta::pure::changetoken::Version\",\n" +
                "      \"version\": \"ftdm:abcdefg456\",\n" +
                "      \"prevVersion\": \"ftdm:abcdefg123\",\n" +
                "      \"changeTokens\": [\n" +
                "        {\n" +
                "          \"@type\": \"meta::pure::changetoken::AddField\",\n" +
                "          \"class\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                "          \"fieldName\": \"abc\",\n" +
                "          \"fieldType\": \"Integer[1]\",\n" +
                "          \"safeCast\": true,\n" +
                "          \"defaultValue\": {\n" +
                "            \"@type\": \"meta::pure::changetoken::ConstValue\",\n" +
                "            \"value\": 100\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}\n");
    }

    @Test
    public void testUpcast() throws JsonProcessingException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        String input =
                "{\n" +
                        "  \"version\":\"ftdm:abcdefg123\", \n" +
                        "  \"@type\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                        "  \"innerObject\": {\"@type\": \"meta::pure::changetoken::tests::SampleClass\"},\n" +
                        "  \"innerNestedArray\":[\n" +
                        "    {\"@type\": \"meta::pure::changetoken::tests::SampleClass\"}, \n" +
                        "    [{\"@type\": \"meta::pure::changetoken::tests::SampleClass\"}]\n" +
                        "  ]\n" +
                        "}";
        Map<String,Object> jsonNode = mapper.readValue(input, Map.class);
        Map<String,Object> jsonNodeOut = (Map<String,Object>) compiledClass.getMethod("upcast", Map.class).invoke(null, jsonNode);

        Map<String,Object> expectedJsonNodeOut = mapper.readValue(
                "{\n" +
                        "  \"version\":\"ftdm:abcdefg456\",\n" +
                        "  \"@type\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                        "  \"innerObject\": {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100},\n" +
                        "  \"innerNestedArray\":[\n" +
                        "    {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100},\n" +
                        "    [{\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100}]\n" +
                        "  ],\n" +
                        "  \"abc\": 100\n" +
                        "}", Map.class); // updated version and new default value field added
        Assert.assertEquals(expectedJsonNodeOut, jsonNodeOut);
        Assert.assertEquals(mapper.readValue(input, Map.class), jsonNode);
    }

    @Test
    public void testDowncast() throws JsonProcessingException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        String input =
                "{\n" +
                        "  \"version\":\"ftdm:abcdefg456\",\n" +
                        "  \"@type\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                        "  \"innerObject\": {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100},\n" +
                        "  \"innerNestedArray\":[\n" +
                        "    {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100},\n" +
                        "    [{\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100}]\n" +
                        "  ],\n" +
                        "  \"abc\": 100\n" +
                        "}";
        Map<String,Object> jsonNode = mapper.readValue(input, Map.class);
        Map<String,Object> jsonNodeOut = (Map<String,Object>) compiledClass.getMethod("downcast", Map.class, String.class)
                .invoke(null, jsonNode, "ftdm:abcdefg123");
        Map<String,Object> expectedJsonNodeOut = mapper.readValue(
                "{\n" +
                        "  \"version\":\"ftdm:abcdefg123\", \n" +
                        "  \"@type\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                        "  \"innerObject\": {\"@type\": \"meta::pure::changetoken::tests::SampleClass\"},\n" +
                        "  \"innerNestedArray\":[\n" +
                        "    {\"@type\": \"meta::pure::changetoken::tests::SampleClass\"}, \n" +
                        "    [{\"@type\": \"meta::pure::changetoken::tests::SampleClass\"}]\n" +
                        "  ]\n" +
                        "}", Map.class); // remove default values
        Assert.assertEquals(expectedJsonNodeOut, jsonNodeOut);
        Assert.assertEquals(mapper.readValue(input, Map.class), jsonNode);
    }

    @Test
    public void testDowncastNull() throws JsonProcessingException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        String input =
                "{\n" +
                        "  \"version\":\"ftdm:abcdefg456\",\n" +
                        "  \"@type\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                        "  \"innerObject\": {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": null},\n" +
                        "  \"innerNestedArray\":[\n" +
                        "    {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": null},\n" +
                        "    [{\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": null}]\n" +
                        "  ],\n" +
                        "  \"abc\": null\n" +
                        "}";
        Map<String,Object> jsonNode = mapper.readValue(input, Map.class);
        Map<String,Object> jsonNodeOut = (Map<String,Object>) compiledClass.getMethod("downcast", Map.class, String.class)
                .invoke(null, jsonNode, "ftdm:abcdefg123");
        Map<String,Object> expectedJsonNodeOut = mapper.readValue(
                "{\n" +
                        "  \"version\":\"ftdm:abcdefg123\", \n" +
                        "  \"@type\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                        "  \"innerObject\": {\"@type\": \"meta::pure::changetoken::tests::SampleClass\"},\n" +
                        "  \"innerNestedArray\":[\n" +
                        "    {\"@type\": \"meta::pure::changetoken::tests::SampleClass\"}, \n" +
                        "    [{\"@type\": \"meta::pure::changetoken::tests::SampleClass\"}]\n" +
                        "  ]\n" +
                        "}", Map.class); // remove default values
        Assert.assertEquals(expectedJsonNodeOut, jsonNodeOut);
        Assert.assertEquals(mapper.readValue(input, Map.class), jsonNode);
    }

    @Test
    public void testDowncastNonDefault() throws JsonProcessingException, NoSuchMethodException
    {
        String input =
                "{\n" +
                        "  \"version\":\"ftdm:abcdefg456\",\n" +
                        "  \"@type\": \"meta::pure::changetoken::tests::SampleClass\",\n" +
                        "  \"innerObject\": {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100},\n" +
                        "  \"innerNestedArray\":[\n" +
                        "    {\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100},\n" +
                        "    [{\"@type\": \"meta::pure::changetoken::tests::SampleClass\", \"abc\": 100}]\n" +
                        "  ],\n" +
                        "  \"abc\": 300\n" +
                        "}";
        Map<String,Object> jsonNode = mapper.readValue(input, Map.class);
        Method downcastMethod = compiledClass.getMethod("downcast", Map.class, String.class);
        InvocationTargetException re = assertThrows("non-default", InvocationTargetException.class, () -> downcastMethod.invoke(null, jsonNode, "ftdm:abcdefg123"));
        Assert.assertEquals("Cannot remove non-default value:300", re.getCause().getMessage());
        Assert.assertEquals(mapper.readValue(input, Map.class), jsonNode);
    }
}
