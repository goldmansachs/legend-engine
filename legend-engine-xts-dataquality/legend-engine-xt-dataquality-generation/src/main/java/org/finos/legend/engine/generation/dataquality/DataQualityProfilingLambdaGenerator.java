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

package org.finos.legend.engine.generation.dataquality;

import org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperValueSpecificationBuilder;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException;
import org.finos.legend.engine.shared.core.operational.errorManagement.ExceptionCategory;
import org.finos.legend.pure.generated.Root_meta_external_dataquality_DataQualityRelationValidation;
import org.finos.legend.pure.generated.core_dataquality_generation_dataprofile;
import org.finos.legend.pure.generated.core_dataquality_generation_samplevalues;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.ConcreteFunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;

public class DataQualityProfilingLambdaGenerator
{

    /**
     * Resolve from a packageable element path. Supports both DataQualityRelationValidation
     * and ConcreteFunctionDefinition (that returns a Relation).
     */
    public static LambdaFunction<?> generateLambda(PureModel pureModel, String qualifiedPath)
    {
        PackageableElement element = pureModel.getPackageableElement(qualifiedPath);
        if (element instanceof Root_meta_external_dataquality_DataQualityRelationValidation)
        {
            return core_dataquality_generation_dataprofile
                    .Root_meta_external_dataquality_dataprofile_getProfilingLambda_DataQualityRelationValidation_1__Boolean_1__LambdaFunction_1_(
                            (Root_meta_external_dataquality_DataQualityRelationValidation) element, true, pureModel.getExecutionSupport());
        }
        if (element instanceof ConcreteFunctionDefinition)
        {
            LambdaFunction<?> pureLambda = core_dataquality_generation_samplevalues
                    .Root_meta_external_dataquality_samplevalues_wrapFunctionDefinitionAsLambda_FunctionDefinition_1__LambdaFunction_1_(
                            (ConcreteFunctionDefinition<?>) element, pureModel.getExecutionSupport());
            return invokePureProfilingLambda(pureModel, pureLambda);
        }
        throw new EngineException(
                "The element at path '" + qualifiedPath + "' is not a DataQualityRelationValidation or ConcreteFunctionDefinition",
                ExceptionCategory.USER_EXECUTION_ERROR);
    }

    /**
     * Generate profiling lambda from an inline protocol LambdaFunction (relation query).
     */
    public static LambdaFunction<?> generateLambdaFromQuery(
            PureModel pureModel,
            org.finos.legend.engine.protocol.pure.m3.function.LambdaFunction query)
    {
        LambdaFunction<?> pureLambda = HelperValueSpecificationBuilder.buildLambda(query, pureModel.getContext());
        return invokePureProfilingLambda(pureModel, pureLambda);
    }

    private static LambdaFunction<?> invokePureProfilingLambda(PureModel pureModel, LambdaFunction<?> pureLambda)
    {
        return core_dataquality_generation_dataprofile
                .Root_meta_external_dataquality_dataprofile_getProfilingLambda_LambdaFunction_1__Boolean_1__LambdaFunction_1_(
                        pureLambda, true, pureModel.getExecutionSupport());
    }

    public static Root_meta_external_dataquality_DataQualityRelationValidation getDataQualityRelationValidation(PureModel pureModel, String qualifiedPath)
    {
        PackageableElement packageableElement = pureModel.getPackageableElement(qualifiedPath);
        if (packageableElement instanceof Root_meta_external_dataquality_DataQualityRelationValidation)
        {
            return (Root_meta_external_dataquality_DataQualityRelationValidation) packageableElement;
        }
        throw new EngineException("The element at path '" + qualifiedPath + "' is not a DataQualityRelationValidation!", ExceptionCategory.USER_EXECUTION_ERROR);
    }
}
