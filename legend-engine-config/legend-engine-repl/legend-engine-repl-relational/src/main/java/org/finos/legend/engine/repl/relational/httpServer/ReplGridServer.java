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

package org.finos.legend.engine.repl.relational.httpServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.collections.api.RichIterable;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.eclipse.collections.api.factory.Lists;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.grammar.to.DEPRECATED_PureGrammarComposerCore;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.result.serialization.SerializationFormat;
import org.finos.legend.engine.plan.execution.stores.relational.result.RelationalResult;
import org.finos.legend.engine.plan.generation.PlanGenerator;
import org.finos.legend.engine.plan.generation.transformers.LegendPlanTransformers;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.ValueSpecification;
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.raw.Lambda;
import org.finos.legend.engine.pure.code.core.PureCoreExtensionLoader;
import org.finos.legend.engine.repl.client.Client;
import org.finos.legend.engine.repl.core.commands.Execute;
import org.finos.legend.engine.shared.core.api.grammar.RenderStyle;
import org.finos.legend.pure.generated.Root_meta_pure_executionPlan_ExecutionPlan;
import org.finos.legend.pure.generated.Root_meta_pure_extension_Extension;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain.Function;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class ReplGridServer
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final PlanExecutor planExecutor;
    private PureModelContextData currentPMCD;
    private String initialResult;
    private Client client;

    public ReplGridServer(Client client)
    {
        this.client = client;
        this.planExecutor = PlanExecutor.newPlanExecutorBuilder().withAvailableStoreExecutors().build();
    }
    
    private static class GridServerResult
    {
        private final String currentQuery;
        private final String result;

        GridServerResult(String currentQuery, String result)
        {
            this.currentQuery = currentQuery;
            this.result = result;
        }

        public String getResult()
        {
            return this.result;
        }

        public String getCurrentQuery()
        {
            return this.currentQuery;
        }
    }

    public boolean canShowGrid()
    {
        return this.initialResult != null;
    }

    public void updateGridState(PureModelContextData pmcd, String result)
    {
        this.currentPMCD = pmcd;
        this.initialResult = result;
    }

    public void initializeServer() throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress((8080)), 0);

        server.createContext("/licenseKey", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                if ("GET".equals(exchange.getRequestMethod()))
                {
                    try
                    {
                        String licenseKey = System.getProperty("legend.repl.grid.licenseKey") == null ? "" : System.getProperty("legend.repl.grid.licenseKey");
                        String key = objectMapper.writeValueAsString(licenseKey);
                        handleResponse(exchange, 200, key);
                    }
                    catch (Exception e)
                    {
                        OutputStream os = exchange.getResponseBody();
                        exchange.sendResponseHeaders(500, e.getMessage().length());
                        os.write(e.getMessage().getBytes(StandardCharsets.UTF_8));
                        os.close();
                    }

                }
            }
        });

        server.createContext("/repl/", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                if ("GET".equals(exchange.getRequestMethod()))
                {
                    try
                    {
                        String[] path = exchange.getRequestURI().getPath().split("/repl/");
                        Path currentPath = Paths.get("").toAbsolutePath();
                        byte[] response = Files.readAllBytes(Paths.get(currentPath.toString() + "/legend-engine-config/legend-engine-repl/legend-engine-repl-relational/target/web-content/package/dist/repl/" + (path[1].equals("grid") ? "index.html" : path[1])));
                        exchange.sendResponseHeaders(200, response.length);
                        OutputStream os = exchange.getResponseBody();
                        os.write(response);
                        os.close();
                    }
                    catch (Exception e)
                    {
                        handleResponse(exchange, 500, e.getMessage());
                    }

                }
            }
        });

        server.createContext("/initialLambda", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                if ("GET".equals(exchange.getRequestMethod()))
                {
                    try
                    {
                        Function func = (Function) currentPMCD.getElements().stream().filter(e -> e.getPath().equals("a::b::c::d__Any_MANY_")).collect(Collectors.toList()).get(0);
                        Lambda lambda = new Lambda();
                        lambda.body = func.body;
                        String response = objectMapper.writeValueAsString(lambda);
                        handleResponse(exchange, 200, response);
                    }
                    catch (Exception e)
                    {
                        handleResponse(exchange, 500, e.getMessage());
                    }
                }
            }
        });

        server.createContext("/gridResult", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                if ("GET".equals(exchange.getRequestMethod()))
                {
                    try
                    {
                        Function func = (Function) currentPMCD.getElements().stream().filter(e -> e.getPath().equals("a::b::c::d__Any_MANY_")).collect(Collectors.toList()).get(0);
                        Lambda lambda = new Lambda();
                        lambda.body = func.body;
                        String lambdaString = lambda.accept(DEPRECATED_PureGrammarComposerCore.Builder.newInstance().withRenderStyle(RenderStyle.PRETTY).build());
                        GridServerResult result = new GridServerResult(lambdaString, initialResult);
                        String response = objectMapper.writeValueAsString(result);
                        handleResponse(exchange, 200, response);
                    }
                    catch (Exception e)
                    {
                        handleResponse(exchange, 500, e.getMessage());
                    }

                }
                else if ("POST".equals(exchange.getRequestMethod()))
                {
                    ValueSpecification funcBody = null;
                    Function func = null;
                    try
                    {
                        InputStreamReader inputStreamReader = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
                        BufferedReader bufferReader = new BufferedReader(inputStreamReader);
                        String requestBody = bufferReader.lines().collect(Collectors.joining());
                        Lambda request = objectMapper.readValue(requestBody, Lambda.class);
                        func = (Function) currentPMCD.getElements().stream().filter(e -> e.getPath().equals("a::b::c::d__Any_MANY_")).collect(Collectors.toList()).get(0);
                        funcBody = func.body.get(0);
                        func.body = request.body;
                        Lambda lambda = new Lambda();
                        lambda.body = Lists.mutable.of(funcBody);
                        String lambdaString = request.accept(DEPRECATED_PureGrammarComposerCore.Builder.newInstance().withRenderStyle(RenderStyle.PRETTY).build());

                        PureModel pureModel = client.getLegendInterface().compile(currentPMCD);
                        RichIterable<? extends Root_meta_pure_extension_Extension> extensions = PureCoreExtensionLoader.extensions().flatCollect(e -> e.extraPureCoreExtensions(pureModel.getExecutionSupport()));
                        if (client.isDebug())
                        {
                            client.getTerminal().writer().println(">> " + extensions.collect(Root_meta_pure_extension_Extension::_type).makeString(", "));
                        }

                        // Plan
                        Root_meta_pure_executionPlan_ExecutionPlan plan = client.getLegendInterface().generatePlan(pureModel, false);
                        String planStr = PlanGenerator.serializeToJSON(plan, "vX_X_X", pureModel, extensions, LegendPlanTransformers.transformers);
                        if (client.isDebug())
                        {
                            client.getTerminal().writer().println(planStr);
                        }

                        // Execute
                        Result res =  planExecutor.execute(planStr);
                        func.body = Lists.mutable.of(funcBody);
                        if (res instanceof RelationalResult)
                        {
                            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                            ((RelationalResult) res).getSerializer(SerializationFormat.DEFAULT).stream(byteArrayOutputStream);
                            OutputStream os = exchange.getResponseBody();
                            GridServerResult result = new GridServerResult(lambdaString, byteArrayOutputStream.toString());
                            String response = objectMapper.writeValueAsString(result);
                            handleResponse(exchange, 200, response);
                        }
                    }
                    catch (Exception e)
                    {
                        System.out.println(e.getMessage());
                        if (func != null)
                        {
                            func.body = Lists.mutable.of(funcBody);
                        }
                        handleResponse(exchange, 500, e.getMessage());
                    }

                }
            }
        });

        server.setExecutor(null);
        server.start();
        System.out.println("REPL Grid Server has started");
    }

    private void handleResponse(HttpExchange exchange, int responseCode, String response) throws IOException
    {
        OutputStream os = exchange.getResponseBody();
        byte[] byteResponse = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(responseCode, byteResponse.length);
        os.write(byteResponse);
        os.close();
    }
}
