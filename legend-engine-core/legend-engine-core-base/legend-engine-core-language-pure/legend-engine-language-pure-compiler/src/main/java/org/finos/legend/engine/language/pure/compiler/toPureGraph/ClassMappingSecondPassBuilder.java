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

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.protocol.pure.m3.SourceInformation;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.AppliedProperty;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.ValueSpecification;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.Variable;
import org.finos.legend.engine.protocol.pure.v1.model.context.EngineErrorType;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.ClassMapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.ClassMappingVisitor;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.OperationClassMapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.PropertyMapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.aggregationAware.AggregationAwareClassMapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.relationFunction.RelationFunctionClassMapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.relationFunction.RelationFunctionEmbeddedPropertyMapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.mapping.relationFunction.RelationFunctionPropertyMapping;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.modelToModel.mapping.PureInstanceClassMapping;
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException;
import org.finos.legend.pure.generated.Root_meta_pure_mapping_SetImplementationContainer_Impl;
import org.finos.legend.pure.generated.Root_meta_pure_metamodel_function_LambdaFunction_Impl;
import org.finos.legend.pure.generated.Root_meta_pure_metamodel_valuespecification_VariableExpression_Impl;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.OperationSetImplementation;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.EmbeddedRelationFunctionSetImplementation;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.RelationFunctionInstanceSetImplementation;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.SetImplementation;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.FunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.generics.GenericType;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.VariableExpression;
import org.finos.legend.pure.m3.navigation.M3Paths;
import org.finos.legend.pure.m3.navigation.function.FunctionDescriptor;
import org.finos.legend.pure.m3.navigation.function.InvalidFunctionDescriptorException;

import java.util.Collections;
import java.util.stream.Collectors;

public class ClassMappingSecondPassBuilder implements ClassMappingVisitor<SetImplementation>
{
    private final CompileContext context;
    private final Mapping parentMapping;

    public ClassMappingSecondPassBuilder(CompileContext context, Mapping parentMapping)
    {
        this.context = context;
        this.parentMapping = parentMapping;
    }

    // NOTE: when we remove this visitor, we can return "void"
    @Override
    public SetImplementation visit(ClassMapping classMapping)
    {
        if (classMapping.extendsClassMappingId != null)
        {
            String superSetId = classMapping.extendsClassMappingId;
            ImmutableSet<SetImplementation> superSets = HelperMappingBuilder.getAllClassMappings(this.parentMapping).select(c -> c._id().equals(superSetId));
            if (superSets.isEmpty())
            {
                throw new EngineException("Can't find extends class mapping '" + superSetId + "' in mapping '" + HelperModelBuilder.getElementFullPath(this.parentMapping, this.context.pureModel.getExecutionSupport()) + "'", classMapping.sourceInformation, EngineErrorType.COMPILATION);
            }
            if (superSets.size() > 1)
            {
                String parents = superSets.stream().map(superSet -> "'" + HelperModelBuilder.getElementFullPath(superSet._parent(), this.context.pureModel.getExecutionSupport()) + "'").sorted().collect(Collectors.joining(", "));
                throw new EngineException("Duplicated class mappings found with ID '" + superSetId + "' in mapping '" + HelperModelBuilder.getElementFullPath(this.parentMapping, this.context.pureModel.getExecutionSupport()) + "'; parent mapping for duplicated: " + parents, classMapping.sourceInformation, EngineErrorType.COMPILATION);
            }
        }
        this.context.getCompilerExtensions().getExtraClassMappingSecondPassProcessors().forEach(processor -> processor.value(classMapping, this.parentMapping, this.context));
        return null;
    }

    @Override
    public SetImplementation visit(OperationClassMapping classMapping)
    {
        OperationSetImplementation operationSetImplementation = (OperationSetImplementation) this.parentMapping._classMappings().detect(c -> c._id().equals(HelperMappingBuilder.getClassMappingId(classMapping, this.context)));
        return operationSetImplementation._parameters(ListIterate.collect(classMapping.parameters, classMappingId ->
        {
            SetImplementation match = HelperMappingBuilder.getAllClassMappings(this.parentMapping).detect(c -> c._id().equals(classMappingId));
            if (match == null)
            {
                throw new EngineException("Can't find class mapping '" + classMappingId + "' in mapping '" + HelperModelBuilder.getElementFullPath(this.parentMapping, this.context.pureModel.getExecutionSupport()) + "'", classMapping.sourceInformation, EngineErrorType.COMPILATION);
            }
            return new Root_meta_pure_mapping_SetImplementationContainer_Impl("", null, context.pureModel.getClass("meta::pure::mapping::SetImplementationContainer"))._id(classMappingId)._setImplementation(match);
        }));
    }

    @Override
    public SetImplementation visit(PureInstanceClassMapping classMapping)
    {
        return this.visit((ClassMapping)classMapping);
    }

    @Override
    public SetImplementation visit(AggregationAwareClassMapping classMapping)
    {
        this.context.getCompilerExtensions().getExtraAggregationAwareClassMappingSecondPassProcessors().forEach(processor -> processor.value(classMapping, this.parentMapping, this.context));
        return null;
    }

    @Override
    public SetImplementation visit(RelationFunctionClassMapping classMapping)
    {
        RelationFunctionInstanceSetImplementation setImpl = (RelationFunctionInstanceSetImplementation) parentMapping._classMappings().detect(c -> c._id().equals(HelperMappingBuilder.getClassMappingId(classMapping, context)));

        // Resolve the source: ~func <descriptor> | ~src <expression>.
        // Either path ends with `_relationFunction` set to a FunctionDefinition whose
        // last expression has generic type Relation<RowType>.
        FunctionDefinition<?> relationFunction;
        if (classMapping.relationFunction != null)
        {
            String functionPath = classMapping.relationFunction.path;
            String functionId;
            try
            {
                functionId = FunctionDescriptor.isValidFunctionDescriptor(functionPath) ? FunctionDescriptor.functionDescriptorToId(functionPath) : functionPath;
            }
            catch (InvalidFunctionDescriptorException e)
            {
                throw new EngineException("Invalid function descriptor specified!", classMapping.relationFunction.sourceInformation, EngineErrorType.COMPILATION, e);
            }
            relationFunction = (FunctionDefinition<?>) context.resolvePackageableElement(functionId, classMapping.sourceInformation);
        }
        else if (classMapping.sourceLambda != null)
        {
            // ~src form: compile the inline expression as a zero-arg LambdaFunction.
            // buildLambdaWithContext gives us a fully-typed M3 lambda whose last-expression
            // generic type carries the row type we need for property mappings.
            relationFunction = HelperValueSpecificationBuilder.buildLambdaWithContext(
                    classMapping.sourceLambda.body,
                    classMapping.sourceLambda.parameters == null ? Lists.fixedSize.empty() : classMapping.sourceLambda.parameters,
                    classMapping.sourceLambda.sourceInformation,
                    context,
                    new ProcessingContext("Building ~src relation source lambda for mapping '" + HelperMappingBuilder.getClassMappingId(classMapping, context) + "'"));
        }
        else
        {
            throw new EngineException("Relation class mapping must specify either '~func' or '~src'.", classMapping.sourceInformation, EngineErrorType.COMPILATION);
        }
        setImpl._relationFunction(relationFunction);

        // Validate that the resolved relation function actually returns Relation<...> BEFORE
        // building any property `_valueFn` lambdas.  Otherwise the row-type extraction below
        // returns null, the per-property lambda falls back to a `$src: Any[1]` parameter, and
        // the user gets a confusing "Can't find property X in class Any" error instead of the
        // legend-pure-equivalent "Relation mapping function should return a Relation" error.
        // MappingValidator still runs the same check later (authoritative for IDE / partial
        // compile paths that don't go through SecondPass) — this is a duplicated *early*
        // check, mirroring the message used by RelationFunctionInstanceSetImplementationValidator
        // in legend-pure.
        org.finos.legend.pure.m3.navigation.ProcessorSupport processorSupport = context.pureModel.getExecutionSupport().getProcessorSupport();
        org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.FunctionType resolvedFnType =
                (org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.FunctionType) processorSupport.function_getFunctionType(relationFunction);
        if (!processorSupport.type_subTypeOf(resolvedFnType._returnType()._rawType(), processorSupport.package_getByUserPath(M3Paths.Relation)))
        {
            throw new EngineException(
                    "Relation mapping function should return a Relation! Found a "
                            + org.finos.legend.pure.m3.navigation.generictype.GenericType.print(resolvedFnType._returnType(), processorSupport)
                            + " instead.",
                    SourceInformationHelper.fromM3SourceInformation(relationFunction.getSourceInformation()),
                    EngineErrorType.COMPILATION);
        }

        // Extract row type from the relation function's last expression.  Validation that the
        // expression actually has type Relation<...> is in MappingValidator.
        GenericType lastExprType = relationFunction._expressionSequence().toList().getLast()._genericType();
        MutableList<? extends GenericType> typeArgs = Lists.mutable.withAll(lastExprType._typeArguments());
        GenericType srcType = typeArgs.isEmpty() ? null : typeArgs.getFirst();

        // Build _valueFn for each property mapping; propagate the relation function
        // (and srcType) to embedded set implementations.
        buildValueFunctionsForPropertyMappings(classMapping.propertyMappings, setImpl, relationFunction, srcType);

        return setImpl;
    }

    /**
     * Walk the protocol property mappings in parallel with the already-built M3 property
     * mappings on {@code parent}, attaching a {@code _valueFn} {@link LambdaFunction} to
     * each {@link org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.RelationFunctionPropertyMapping}.
     * For embedded mappings, the relation function and srcType are propagated unchanged
     * (embedded sets share the parent's row type).
     */
    private void buildValueFunctionsForPropertyMappings(java.util.List<PropertyMapping> protocolPropertyMappings, org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.PropertyMappingsImplementation parent, FunctionDefinition<?> relationFunction, GenericType srcType)
    {
        if (protocolPropertyMappings == null || protocolPropertyMappings.isEmpty())
        {
            return;
        }
        MutableList<? extends org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.PropertyMapping> m3PropertyMappings = Lists.mutable.withAll(parent._propertyMappings());
        for (int i = 0; i < protocolPropertyMappings.size(); i++)
        {
            PropertyMapping protocolPm = protocolPropertyMappings.get(i);
            org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.PropertyMapping m3Pm = m3PropertyMappings.get(i);
            if (protocolPm instanceof RelationFunctionPropertyMapping && m3Pm instanceof org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.RelationFunctionPropertyMapping)
            {
                RelationFunctionPropertyMapping pPm = (RelationFunctionPropertyMapping) protocolPm;
                org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.RelationFunctionPropertyMapping mPm = (org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.RelationFunctionPropertyMapping) m3Pm;
                LambdaFunction<?> valueFn = buildPropertyValueFn(pPm, srcType, parent._id());
                if (valueFn != null)
                {
                    mPm._valueFn(valueFn);
                }
            }
            else if (protocolPm instanceof RelationFunctionEmbeddedPropertyMapping && m3Pm instanceof EmbeddedRelationFunctionSetImplementation)
            {
                RelationFunctionEmbeddedPropertyMapping pEmb = (RelationFunctionEmbeddedPropertyMapping) protocolPm;
                EmbeddedRelationFunctionSetImplementation mEmb = (EmbeddedRelationFunctionSetImplementation) m3Pm;
                mEmb._relationFunction(relationFunction);
                buildValueFunctionsForPropertyMappings(pEmb.propertyMappings, mEmb, relationFunction, srcType);
            }
        }
    }

    /**
     * Synthesise the {@code _valueFn} lambda for a single property mapping.  Two protocol
     * shapes are supported:
     * <ul>
     *   <li>bare column ({@code propName: COL}) — lowered to {@code { $src.COL}} so that
     *       downstream consumers can treat both forms uniformly.</li>
     *   <li>inline expression ({@code propName: $src.A + $src.B}) — the protocol Lambda
     *       (built by the grammar walker as a zero-arg wrapper around the user expression)
     *       is compiled with {@code src} typed at the relation function's row type.</li>
     * </ul>
     * Returns {@code null} if neither shape is set — the property mapping is invalid and
     * will be reported by {@link org.finos.legend.engine.language.pure.compiler.toPureGraph.validator.MappingValidator}.
     */
    private LambdaFunction<?> buildPropertyValueFn(RelationFunctionPropertyMapping pm, GenericType srcType, String parentId)
    {
        // Pick the protocol AST to compile.  For the bare-column form we synthesise an
        // AppliedProperty against `$src`; for the inline-expression form we use the
        // walker-built lambda body verbatim.
        java.util.List<ValueSpecification> body;
        if (pm.valueFn != null && pm.valueFn.body != null && !pm.valueFn.body.isEmpty())
        {
            body = pm.valueFn.body;
        }
        else if (pm.column != null && !pm.column.isEmpty())
        {
            Variable srcRef = new Variable();
            srcRef.name = "src";
            srcRef.sourceInformation = pm.sourceInformation;
            AppliedProperty colAccess = new AppliedProperty();
            colAccess.property = pm.column;
            colAccess.parameters = Collections.singletonList(srcRef);
            colAccess.sourceInformation = pm.sourceInformation;
            body = Collections.singletonList(colAccess);
        }
        else
        {
            return null;
        }
        return compileRelationPropertyLambda(body, srcType, parentId + "." + (pm.property == null ? "" : pm.property.property), pm.sourceInformation);
    }

    /**
     * Compile a body of {@link ValueSpecification}s as a single-arg lambda whose only
     * parameter is {@code src} bound to {@code srcType}.  Mirrors the pattern used by
     * {@code HelperMappingBuilder.processPurePropertyMappingTransform} for M2M
     * transform lambdas.
     */
    private LambdaFunction<?> compileRelationPropertyLambda(java.util.List<ValueSpecification> body, GenericType srcType, String lambdaId, SourceInformation sourceInformation)
    {
        VariableExpression srcVar = new Root_meta_pure_metamodel_valuespecification_VariableExpression_Impl("", null, context.pureModel.getClass(M3Paths.VariableExpression))
                ._name("src")
                ._multiplicity(context.pureModel.getMultiplicity("one"))
                ._genericType(srcType == null
                        // Fallback when the relation function's row type can't be inferred (e.g. ~func
                        // resolved to a function whose return type isn't Relation<...>).  The validator
                        // will surface that as a separate error; here we degrade gracefully.
                        ? context.newGenericType(context.pureModel.getType(M3Paths.Any))
                        : (GenericType) org.finos.legend.pure.m3.navigation.generictype.GenericType.copyGenericType(srcType, context.pureModel.getExecutionSupport().getProcessorSupport()));

        MutableList<VariableExpression> pureParameters = Lists.mutable.with(srcVar);
        ProcessingContext ctx = new ProcessingContext("Building relation property valueFn for '" + lambdaId + "'");
        ctx.addVariableLevel();
        ctx.addInferredVariables("src", srcVar);
        MutableList<String> openVariables = Lists.mutable.empty();
        MutableList<org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.ValueSpecification> valueSpecifications =
                ListIterate.collect(body, p -> p.accept(new ValueSpecificationBuilder(context, openVariables, ctx)));
        MutableList<String> cleanedOpenVariables = openVariables.distinct();
        cleanedOpenVariables.removeAll(pureParameters.collect(VariableExpression::_name));
        GenericType functionType = PureModel.buildFunctionType(pureParameters, valueSpecifications.getLast()._genericType(), valueSpecifications.getLast()._multiplicity(), context.pureModel);
        ctx.flushVariable("src");
        ctx.removeLastVariableLevel();

        return new Root_meta_pure_metamodel_function_LambdaFunction_Impl<>(lambdaId, SourceInformationHelper.toM3SourceInformation(sourceInformation), null)
                ._classifierGenericType(context.newGenericType(context.pureModel.getType(M3Paths.LambdaFunction), Lists.mutable.with(functionType)))
                ._openVariables(cleanedOpenVariables)
                ._expressionSequence(valueSpecifications);
    }
}
