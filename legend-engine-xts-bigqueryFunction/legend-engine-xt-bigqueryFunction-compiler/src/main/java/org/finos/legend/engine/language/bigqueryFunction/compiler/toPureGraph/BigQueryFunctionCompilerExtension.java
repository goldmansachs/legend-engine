// Copyright 2023 Goldman Sachs
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

package org.finos.legend.engine.language.bigqueryFunction.compiler.toPureGraph;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.code.core.CoreFunctionActivatorCodeRepositoryProvider;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.CompileContext;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.CompilerExtension;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.extension.Processor;
import org.finos.legend.engine.protocol.bigqueryFunction.metamodel.BigQueryFunction;
import org.finos.legend.engine.protocol.bigqueryFunction.metamodel.BigQueryFunctionDeploymentConfiguration;
import org.finos.legend.engine.protocol.functionActivator.metamodel.DeploymentOwner;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.connection.PackageableConnection;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain.Function;
import org.finos.legend.pure.generated.*;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.PackageableFunction;
import org.finos.legend.pure.m3.navigation.function.FunctionDescriptor;

public class BigQueryFunctionCompilerExtension implements CompilerExtension
{
    // Here only for dependency check error ...
    CoreFunctionActivatorCodeRepositoryProvider forDependencies;

    @Override
    public MutableList<String> group()
    {
        return org.eclipse.collections.impl.factory.Lists.mutable.with("Function_Activator", "BigQuery");
    }

    @Override
    public CompilerExtension build()
    {
        return new BigQueryFunctionCompilerExtension();
    }

    @Override
    public Iterable<? extends Processor<?>> getExtraProcessors()
    {
        return Lists.fixedSize.of(
                Processor.newProcessor(
                        BigQueryFunction.class,
                        Lists.fixedSize.with(BigQueryFunctionDeploymentConfiguration.class, Function.class),
                        this::buildBigQueryFunction
                ),
                Processor.newProcessor(
                        BigQueryFunctionDeploymentConfiguration.class,
                        Lists.fixedSize.with(PackageableConnection.class),
                        this::buildDeploymentConfig
                )
        );
    }

    public Root_meta_external_function_activator_bigQueryFunction_BigQueryFunction buildBigQueryFunction(BigQueryFunction bigQueryFunction, CompileContext context)
    {
        try
        {
            PackageableFunction<?> func = (PackageableFunction<?>) context.resolvePackageableElement(FunctionDescriptor.functionDescriptorToId(bigQueryFunction.function.path), bigQueryFunction.sourceInformation);
            return new Root_meta_external_function_activator_bigQueryFunction_BigQueryFunction_Impl(
                    bigQueryFunction.name,
                    null,
                    context.pureModel.getClass("meta::external::function::activator::bigQueryFunction::BigQueryFunction"))
                    ._stereotypes(ListIterate.collect(bigQueryFunction.stereotypes, s -> context.resolveStereotype(s.profile, s.value, s.profileSourceInformation, s.sourceInformation)))
                    ._taggedValues(ListIterate.collect(bigQueryFunction.taggedValues, t -> new Root_meta_pure_metamodel_extension_TaggedValue_Impl("", null, context.pureModel.getClass("meta::pure::metamodel::extension::TaggedValue"))._tag(context.resolveTag(t.tag.profile, t.tag.value, t.tag.profileSourceInformation, t.tag.sourceInformation))._value(t.value)))
                    ._functionName(bigQueryFunction.functionName)
                    ._function(func)
                    ._description(bigQueryFunction.description)
                    ._ownership(new Root_meta_external_function_activator_DeploymentOwnership_Impl("")._id(((DeploymentOwner)bigQueryFunction.ownership).id))
                    ._activationConfiguration(bigQueryFunction.activationConfiguration != null ? buildDeploymentConfig((BigQueryFunctionDeploymentConfiguration) bigQueryFunction.activationConfiguration, context) : null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Root_meta_external_function_activator_bigQueryFunction_BigQueryFunctionDeploymentConfiguration buildDeploymentConfig(BigQueryFunctionDeploymentConfiguration configuration, CompileContext context)
    {
        return new Root_meta_external_function_activator_bigQueryFunction_BigQueryFunctionDeploymentConfiguration_Impl("")
                ._target((Root_meta_external_store_relational_runtime_RelationalDatabaseConnection) context.resolveConnection(configuration.activationConnection.connection, configuration.sourceInformation));
    }
}