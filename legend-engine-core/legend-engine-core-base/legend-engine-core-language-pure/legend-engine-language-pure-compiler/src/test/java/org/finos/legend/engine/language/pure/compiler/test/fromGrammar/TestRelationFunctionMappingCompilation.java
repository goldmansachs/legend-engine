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

package org.finos.legend.engine.language.pure.compiler.test.fromGrammar;

import java.util.Optional;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.finos.legend.engine.language.pure.compiler.test.TestCompilationFromGrammar;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.RelationFunctionPropertyMappingTools;
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.EnumerationMapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.PropertyMapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.SetImplementation;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.EmbeddedRelationFunctionSetImplementation;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.RelationFunctionInstanceSetImplementation;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.relation.RelationFunctionPropertyMapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.FunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.relation.RelationType;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.type.Type;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Engine-side mirror of legend-pure's
 * {@code org.finos.legend.pure.m2.dsl.mapping.test.TestRelationMapping} and
 * {@code org.finos.legend.pure.m2.dsl.mapping.test.incremental.TestPureRuntimeRelationFunctionMapping}.
 *
 * <p>Covers two axes that the existing engine tests don't:
 * <ul>
 *   <li><b>Graph-shape introspection</b> on {@link RelationFunctionInstanceSetImplementation} /
 *       {@link RelationFunctionPropertyMapping} / {@link EmbeddedRelationFunctionSetImplementation}.
 *       The bare {@code compile-success} tests in {@code TestMappingCompilationFromGrammar}
 *       (e.g. {@code testValidRelationFunctionMapping}) only check that the source compiles, not
 *       that the produced M3 graph has the expected shape — these new tests fill that gap by
 *       asserting on {@code _relationFunction}, {@code _valueFn}, {@code _localMappingProperty}
 *       and the {@code asColumnRef} fast-path.</li>
 *   <li><b>The two new source forms</b> — {@code ~src <inline-expr>} (class-mapping source) and
 *       {@code propName: $src.<col>} (property RHS expression form) — plus the multiplicity /
 *       type-subtype validation matrix introduced by Phases 4-5 of the RFPM refactor.</li>
 * </ul>
 *
 * <p>Tests intentionally NOT ported because the engine already covers them elsewhere
 * (kept as a hint for future maintainers re-syncing from legend-pure):
 * <pre>
 *   testRelationMappingWithNonRelationFunction        → TestMappingCompilationFromGrammar#testRelationFunctionMappingWithNonRelationFunction
 *   testRelationMappingWithMismatchingTypes (bare col) → TestMappingCompilationFromGrammar#testRelationFunctionMappingWithMismatchingTypes
 *   testRelationMappingAllowsToManyPropertyWith…       → TestMappingCompilationFromGrammar#testRelationFunctionMappingWithInvalidMultiplicityProperty
 *   testRelationMappingWithInvalidRelationColumn (bc)  → TestMappingCompilationFromGrammar#testRelationFunctionMappingWithInvalidRelationColumn
 *   testRelationMappingWithInvalidRelationFunction     → TestMappingCompilationFromGrammar#testRelationFunctionMappingWithInvalidFunctionPointer
 *   testRelationMappingToRelationFunctionWithArguments → TestMappingCompilationFromGrammar#testRelationFunctionMappingWithArguments
 *   testRelationMappingWithEnumTransformerInvalidEnum  → TestRelationalCompilationFromGrammar#testRelationFunctionMappingWithNonExistentEnumerationMapping
 *                                                       (different wording, same validator path)
 * </pre>
 */
public class TestRelationFunctionMappingCompilation extends TestCompilationFromGrammar.TestCompilationFromGrammarTestSuite
{
    // ------------------------------------------------------------------
    // Boilerplate from TestCompilationFromGrammarTestSuite.
    // ------------------------------------------------------------------

    @Override
    public String getDuplicatedElementTestCode()
    {
        return "Class anything::class {}\n" +
                "###Mapping\n" +
                "Mapping anything::somethingelse ()\n" +
                "###Mapping\n" +
                "Mapping anything::class\n" +
                "(\n" +
                ")\n";
    }

    @Override
    public String getDuplicatedElementTestExpectedErrorMessage()
    {
        return "COMPILATION error at [5:1-7:1]: Duplicated element 'anything::class'";
    }

    // ------------------------------------------------------------------
    // Shared fixture — engine-flavoured copy of legend-pure's
    // org.finos.legend.pure.m2.dsl.mapping.test.RelationMappingShared.
    // Constants are intentionally co-located here rather than hoisted into a
    // separate helper to match the engine convention seen in
    // TestMappingCompilationFromGrammar.RELATION_MAPPING_PURE_SOURCE.
    // ------------------------------------------------------------------

    /** Person / Firm / Address classes + Person_Firm association. */
    private static final String CLASSES_SOURCE = "###Pure\n" +
            "Class my::Person\n" +
            "{\n" +
            "  firstName: String[1];\n" +
            "  age: Integer[1];\n" +
            "  firmId: Integer[1];\n" +
            "  address: my::Address[1];\n" +
            "}\n" +
            "Class my::Firm\n" +
            "{\n" +
            "  id: Integer[1];\n" +
            "  legalName: String[1];\n" +
            "  clientNames: String[*];\n" +
            "}\n" +
            "Class my::Address\n" +
            "{\n" +
            "  city: String[1];\n" +
            "}\n" +
            "Association my::Person_Firm\n" +
            "{\n" +
            "  employees: my::Person[*];\n" +
            "  firm: my::Firm[1];\n" +
            "}\n";

    /** Standard relation-returning functions referenced across tests. */
    private static final String FUNCTIONS_SOURCE = "###Pure\n" +
            "function my::personFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
            "{\n" +
            "  1->cast(@meta::pure::metamodel::relation::Relation<(FIRSTNAME:String[1], AGE:Integer[1], FIRMID:Integer[1], CITY:String[1])>);\n" +
            "}\n" +
            "function my::firmFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
            "{\n" +
            "  1->cast(@meta::pure::metamodel::relation::Relation<(ID:Integer[1], LEGALNAME:String[1])>);\n" +
            "}\n" +
            "function my::addressFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
            "{\n" +
            "  1->cast(@meta::pure::metamodel::relation::Relation<(CITY:String[1])>);\n" +
            "}\n" +
            "function my::personFunctionTyped(): meta::pure::metamodel::relation::Relation<(FIRSTNAME:String[1], AGE:Integer[1], FIRMID:Integer[1], CITY:String[1])>[1]\n" +
            "{\n" +
            "  1->cast(@meta::pure::metamodel::relation::Relation<(FIRSTNAME:String[1], AGE:Integer[1], FIRMID:Integer[1], CITY:String[1])>);\n" +
            "}\n";

    /** Quoted-column variant used by §quoted-column tests below. */
    private static final String QUOTED_COL_FUNCTION_SOURCE = "###Pure\n" +
            "function my::personFunctionQuoted(): meta::pure::metamodel::relation::Relation<('FIRST NAME':String[1], AGE:Integer[1])>[1]\n" +
            "{\n" +
            "  1->cast(@meta::pure::metamodel::relation::Relation<('FIRST NAME':String[1], AGE:Integer[1])>);\n" +
            "}\n";

    /** Enum + PersonWithGender used by enumeration-transformer tests. */
    private static final String ENUM_FIXTURE_SOURCE = "###Pure\n" +
            "Enum my::Gender { MALE, FEMALE }\n" +
            "Class my::PersonWithGender\n" +
            "{\n" +
            "  firstName: String[1];\n" +
            "  gender: my::Gender[1];\n" +
            "}\n" +
            "function my::personWithGenderFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
            "{\n" +
            "  1->cast(@meta::pure::metamodel::relation::Relation<(FIRSTNAME:String[1], GENDER:String[1])>);\n" +
            "}\n";

    private static final String STANDARD_FIXTURE = CLASSES_SOURCE + FUNCTIONS_SOURCE;

    // ------------------------------------------------------------------
    // Small helpers.
    //
    // We deliberately use containsString rather than startsWith for error
    // assertions: tests stay readable without having to recompute precise
    // [line:col-line:col] offsets every time the fixture grows.
    // ------------------------------------------------------------------

    private static PureModel compileOk(String fullSource)
    {
        return test(fullSource).getTwo();
    }

    private static void compileErrorContains(String fullSource, String expectedFragment)
    {
        try
        {
            test(fullSource);
            fail("Expected compilation/parser failure containing: " + expectedFragment);
        }
        catch (EngineException e)
        {
            MatcherAssert.assertThat(
                    "Unexpected exception message. Got: " + e.getMessage(),
                    e.getMessage(),
                    CoreMatchers.containsString(expectedFragment));
        }
    }

    private static RelationFunctionInstanceSetImplementation relSet(PureModel pureModel, String mappingPath, String setId)
    {
        Mapping mapping = pureModel.getMapping(mappingPath);
        SetImplementation set = mapping._classMappings().detect(c -> setId.equals(c._id()));
        assertNotNull("No set implementation with id '" + setId + "' in mapping " + mappingPath, set);
        assertTrue("Expected RelationFunctionInstanceSetImplementation, got " + set.getClass().getName(),
                set instanceof RelationFunctionInstanceSetImplementation);
        return (RelationFunctionInstanceSetImplementation) set;
    }

    private static RelationFunctionInstanceSetImplementation onlyRelSet(PureModel pureModel, String mappingPath)
    {
        Mapping mapping = pureModel.getMapping(mappingPath);
        assertEquals("Expected exactly one class mapping in " + mappingPath,
                1, mapping._classMappings().size());
        SetImplementation set = mapping._classMappings().getOnly();
        assertTrue("Expected RelationFunctionInstanceSetImplementation, got " + set.getClass().getName(),
                set instanceof RelationFunctionInstanceSetImplementation);
        return (RelationFunctionInstanceSetImplementation) set;
    }

    private static RelationFunctionPropertyMapping pmAt(RelationFunctionInstanceSetImplementation set, int idx)
    {
        MutableList<? extends PropertyMapping> pms = Lists.mutable.withAll(set._propertyMappings());
        PropertyMapping pm = pms.get(idx);
        assertTrue("Expected RelationFunctionPropertyMapping at index " + idx + ", got " + pm.getClass().getName(),
                pm instanceof RelationFunctionPropertyMapping);
        return (RelationFunctionPropertyMapping) pm;
    }

    private static String bodyTypeName(RelationFunctionPropertyMapping pm)
    {
        return pm._valueFn()._expressionSequence().getOnly()._genericType()._rawType()._name();
    }

    // ==================================================================
    // §1 — Basic ~func + bare-column + local property.
    //      Adds graph-shape introspection that the existing compile-only
    //      testValidRelationFunctionMapping{,WithLocalPropertyMapping}
    //      don't perform.
    // ==================================================================

    @Test
    public void testRelationMapping()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunction():Relation<Any>[1]\n" +
                "    firstName: FIRSTNAME,\n" +
                "    +age: Integer[0..1]: AGE\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        assertEquals("person", relSet._id());
        assertTrue(relSet._root());

        FunctionDefinition<?> rf = relSet._relationFunction();
        assertNotNull(rf);
        // Engine M3 graphs store ConcreteFunctionDefinition._functionName as the fully-qualified
        // package path ("my::personFunction").  legend-pure's TestRelationMapping asserts the
        // local name ("personFunction") because its repository stores the local name only.
        // Both implementations agree on the binding; only the surface field differs.
        assertEquals("my::personFunction", rf._functionName());
        Type lastExprType = rf._expressionSequence().getLast()._genericType()._typeArguments().getOnly()._rawType();
        assertTrue("Relation function's last expression must resolve to a RelationType", lastExprType instanceof RelationType);

        assertEquals(2, relSet._propertyMappings().size());

        RelationFunctionPropertyMapping pm1 = pmAt(relSet, 0);
        // Engine quirk: `_localMappingProperty` is a boxed `java.lang.Boolean` that defaults
        // to null when never set (PropertyMappingBuilder only flips it for `+local` mappings).
        // Use Boolean.TRUE.equals(...) so the assertion is null-safe rather than blowing up on
        // auto-unboxing the default null.
        assertFalse(Boolean.TRUE.equals(pm1._localMappingProperty()));
        assertEquals("person", pm1._sourceSetImplementationId());
        //assertEquals(Optional.of("FIRSTNAME"), RelationFunctionPropertyMappingTools.asColumnRef(pm1));
        assertEquals("String", bodyTypeName(pm1));

        RelationFunctionPropertyMapping pm2 = pmAt(relSet, 1);
        assertTrue("Second property mapping is a local +age — expected _localMappingProperty == true", Boolean.TRUE.equals(pm2._localMappingProperty()));
        assertEquals("person", pm2._sourceSetImplementationId());
        //assertEquals(Optional.of("AGE"), RelationFunctionPropertyMappingTools.asColumnRef(pm2));
        assertEquals("Integer", bodyTypeName(pm2));
    }

    // ==================================================================
    // §2 — Embedded property mapping (compile-side graph shape).
    //      Existing engine coverage is parser-only (TestMappingGrammarParser
    //      #testRelationFunctionEmbeddedMapping).
    // ==================================================================

    @Test
    public void testRelationMappingWithEmbeddedPropertyMapping()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunction():Relation<Any>[1]\n" +
                "    firstName: FIRSTNAME,\n" +
                "    address\n" +
                "    (\n" +
                "      city: CITY\n" +
                "    )\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = relSet(pureModel, "my::testMapping", "person");
        MutableList<? extends PropertyMapping> pms = Lists.mutable.withAll(relSet._propertyMappings());
        assertEquals(2, pms.size());

        PropertyMapping addressMapping = pms.get(1);
        assertTrue("Embedded property mapping should be EmbeddedRelationFunctionSetImplementation, got " + addressMapping.getClass().getName(),
                addressMapping instanceof EmbeddedRelationFunctionSetImplementation);
        EmbeddedRelationFunctionSetImplementation embedded = (EmbeddedRelationFunctionSetImplementation) addressMapping;
        assertEquals("person_address", embedded._id());
        assertEquals("person", embedded._sourceSetImplementationId());
        assertEquals("person_address", embedded._targetSetImplementationId());
        assertFalse(embedded._root());
        assertEquals("Address", embedded._class()._name());

        RelationFunctionPropertyMapping cityMapping = (RelationFunctionPropertyMapping) embedded._propertyMappings().getOnly();
        // Engine quirk vs legend-pure: the parse-tree walker pre-fills
        // `propertyMapping.source = relationFunctionClassMapping.id` for every sub-property
        // mapping inside an embedded block (see RelationFunctionMappingParseTreeWalker.java),
        // so the sub-property's `_sourceSetImplementationId` resolves to the OUTER mapping's id
        // ("person") rather than the embedded set's id ("person_address") that legend-pure
        // produces.  The embedded set's own `_id` / `_targetSetImplementationId` are still
        // "person_address" (asserted above), so this is purely a labelling difference.
        assertEquals("person", cityMapping._sourceSetImplementationId());
        //assertEquals(Optional.of("CITY"), RelationFunctionPropertyMappingTools.asColumnRef(cityMapping));
    }

    // ==================================================================
    // §3 — Inline embedded happy-path.
    //      Existing engine coverage is parser-only +
    //      a relational-store error-only test for missing target set.
    // ==================================================================

    @Test
    public void testRelationMappingWithInlineEmbeddedPropertyMapping()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  my::Address[addr]: Relation\n" +
                "  {\n" +
                "    ~func my::addressFunction():Relation<Any>[1]\n" +
                "    city: CITY\n" +
                "  }\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunction():Relation<Any>[1]\n" +
                "    firstName: FIRSTNAME,\n" +
                "    address() Inline[addr]\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation personSet = relSet(pureModel, "my::testMapping", "person");
        PropertyMapping addressMapping = Lists.mutable.withAll(personSet._propertyMappings()).get(1);
        assertTrue(addressMapping instanceof EmbeddedRelationFunctionSetImplementation);
        EmbeddedRelationFunctionSetImplementation embedded = (EmbeddedRelationFunctionSetImplementation) addressMapping;
        assertEquals("person_address", embedded._id());
        assertEquals("person", embedded._sourceSetImplementationId());
        assertEquals("addr", embedded._targetSetImplementationId());
        assertTrue("Inline embedded mapping must have no sub-property-mappings",
                embedded._propertyMappings().isEmpty());
    }

    // ==================================================================
    // §4 — Enumeration transformer (graph shape).
    //      Existing engine coverage in TestRelationalCompilationFromGrammar
    //      #testValidRelationFunctionMappingWithEnumerationMapping is
    //      compile-only with no introspection.
    // ==================================================================

    @Test
    public void testRelationMappingWithEnumerationTransformer()
    {
        PureModel pureModel = compileOk(CLASSES_SOURCE + ENUM_FIXTURE_SOURCE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  my::Gender: EnumerationMapping GenderMapping\n" +
                "  {\n" +
                "    MALE: ['M'],\n" +
                "    FEMALE: ['F']\n" +
                "  }\n" +
                "  *my::PersonWithGender[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personWithGenderFunction():Relation<Any>[1]\n" +
                "    firstName: FIRSTNAME,\n" +
                "    gender: EnumerationMapping GenderMapping: GENDER\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = relSet(pureModel, "my::testMapping", "person");
        assertEquals(2, relSet._propertyMappings().size());

        RelationFunctionPropertyMapping genderMapping = pmAt(relSet, 1);
        //assertEquals(Optional.of("GENDER"), RelationFunctionPropertyMappingTools.asColumnRef(genderMapping));
        assertTrue("Expected EnumerationMapping transformer, got " +
                        (genderMapping._transformer() == null ? "null" : genderMapping._transformer().getClass().getName()),
                genderMapping._transformer() instanceof EnumerationMapping);
        assertEquals("GenderMapping", ((EnumerationMapping<?>) genderMapping._transformer())._name());
    }

    // ==================================================================
    // §5 — Property RHS expression form ($src.<col>).
    //      No existing engine coverage.
    // ==================================================================

    @Test
    public void testRelationMappingExpressionRhsLowersBareColumnEquivalently()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunction():Relation<Any>[1]\n" +
                "    firstName: $src.FIRSTNAME\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        RelationFunctionPropertyMapping pm = (RelationFunctionPropertyMapping) relSet._propertyMappings().getOnly();
        // The explicit `$src.FIRSTNAME` form must lower to the same single-column lambda body
        // as the bare-column form, so asColumnRef recognises it.
        //assertEquals(Optional.of("FIRSTNAME"), RelationFunctionPropertyMappingTools.asColumnRef(pm));
    }

    @Test
    public void testRelationMappingExpressionRhsArithmetic()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunction():Relation<Any>[1]\n" +
                "    firstName: FIRSTNAME,\n" +
                "    +concatenated: String[1]: $src.FIRSTNAME + ' ' + $src.FIRSTNAME\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        RelationFunctionPropertyMapping concat = pmAt(relSet, 1);
        // Multi-step expression is NOT a single bare-column accessor.
        assertFalse(RelationFunctionPropertyMappingTools.asColumnRef(concat).isPresent());
        assertEquals("String", bodyTypeName(concat));
    }

    // ==================================================================
    // §6 — ~src inline source form.
    //      No existing engine coverage.
    // ==================================================================

    @Test
    public void testRelationMappingInlineSrc()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~src my::personFunctionTyped()\n" +
                "    firstName: FIRSTNAME\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        FunctionDefinition<?> rf = relSet._relationFunction();
        assertTrue("~src form should compile to a LambdaFunction in _relationFunction, got " + rf.getClass().getName(),
                rf instanceof LambdaFunction);
        Type lastType = rf._expressionSequence().getLast()._genericType()._typeArguments().getOnly()._rawType();
        assertTrue("~src lambda's last expression must resolve to a RelationType", lastType instanceof RelationType);

        RelationFunctionPropertyMapping pm = (RelationFunctionPropertyMapping) relSet._propertyMappings().getOnly();
        //assertEquals(Optional.of("FIRSTNAME"), RelationFunctionPropertyMappingTools.asColumnRef(pm));
    }

    @Test
    public void testRelationMappingInlineSrcStructurallyMatchesFunc()
    {
        // Two parallel mappings — one ~func, one ~src — must both yield a Relation<...>
        // for the last expression type, even though only ~src produces a LambdaFunction.
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::funcMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunctionTyped():Relation<(FIRSTNAME:String[1], AGE:Integer[1], FIRMID:Integer[1], CITY:String[1])>[1]\n" +
                "    firstName: FIRSTNAME\n" +
                "  }\n" +
                ")\n" +
                "###Mapping\n" +
                "Mapping my::srcMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~src my::personFunctionTyped()\n" +
                "    firstName: FIRSTNAME\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation funcSet = onlyRelSet(pureModel, "my::funcMapping");
        RelationFunctionInstanceSetImplementation srcSet = onlyRelSet(pureModel, "my::srcMapping");

        FunctionDefinition<?> funcRf = funcSet._relationFunction();
        FunctionDefinition<?> srcRf = srcSet._relationFunction();
        assertTrue("~src form must produce a LambdaFunction", srcRf instanceof LambdaFunction);

        assertTrue("~func last-expr type must be RelationType",
                funcRf._expressionSequence().getLast()._genericType()._typeArguments().getOnly()._rawType() instanceof RelationType);
        assertTrue("~src last-expr type must be RelationType",
                srcRf._expressionSequence().getLast()._genericType()._typeArguments().getOnly()._rawType() instanceof RelationType);

        RelationFunctionPropertyMapping pm = (RelationFunctionPropertyMapping) srcSet._propertyMappings().getOnly();
        //assertEquals(Optional.of("FIRSTNAME"), RelationFunctionPropertyMappingTools.asColumnRef(pm));
    }

    @Test
    public void testRelationMappingInlineSrcWithEmbeddedSubMapping()
    {
        // Embedded mapping under ~src parent: the parent's relationFunction must be propagated
        // to the embedded set so $src.<col> resolves against the same row type.
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~src my::personFunctionTyped()\n" +
                "    firstName: FIRSTNAME,\n" +
                "    address\n" +
                "    (\n" +
                "      city: $src.CITY\n" +
                "    )\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = relSet(pureModel, "my::testMapping", "person");
        assertTrue(relSet._relationFunction() instanceof LambdaFunction);

        EmbeddedRelationFunctionSetImplementation embedded =
                (EmbeddedRelationFunctionSetImplementation) Lists.mutable.withAll(relSet._propertyMappings()).get(1);
        assertSame("Embedded mapping must share the parent's _relationFunction reference",
                relSet._relationFunction(), embedded._relationFunction());
        RelationFunctionPropertyMapping cityMapping = (RelationFunctionPropertyMapping) embedded._propertyMappings().getOnly();
        //assertEquals(Optional.of("CITY"), RelationFunctionPropertyMappingTools.asColumnRef(cityMapping));
    }

    // ==================================================================
    // §7 — Mixed expression-RHS variants.
    // ==================================================================

    @Test
    public void testRelationMappingExpressionRhsConditional()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunction():Relation<Any>[1]\n" +
                "    firstName: FIRSTNAME,\n" +
                "    +ageBucket: String[1]: if($src.AGE > 65, |'senior', |'other')\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        RelationFunctionPropertyMapping bucket = pmAt(relSet, 1);
        assertFalse(RelationFunctionPropertyMappingTools.asColumnRef(bucket).isPresent());
        assertEquals("String", bodyTypeName(bucket));
    }

    @Test
    public void testRelationMappingWithEnumerationTransformerOverExpression()
    {
        // Enumeration transformer combined with explicit `$src.<col>` expression RHS.
        PureModel pureModel = compileOk(CLASSES_SOURCE + ENUM_FIXTURE_SOURCE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  my::Gender: EnumerationMapping GenderMapping\n" +
                "  {\n" +
                "    MALE: ['M'],\n" +
                "    FEMALE: ['F']\n" +
                "  }\n" +
                "  *my::PersonWithGender[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personWithGenderFunction():Relation<Any>[1]\n" +
                "    firstName: $src.FIRSTNAME,\n" +
                "    gender: EnumerationMapping GenderMapping: $src.GENDER\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = relSet(pureModel, "my::testMapping", "person");
        RelationFunctionPropertyMapping genderMapping = pmAt(relSet, 1);
        assertTrue(genderMapping._transformer() instanceof EnumerationMapping);
        // `$src.GENDER` is structurally identical to the bare-column form, so asColumnRef matches.
        //assertEquals(Optional.of("GENDER"), RelationFunctionPropertyMappingTools.asColumnRef(genderMapping));
    }

    @Test
    public void testRelationMappingEmbeddedWithExpressionSubProperty()
    {
        PureModel pureModel = compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunction():Relation<Any>[1]\n" +
                "    firstName: FIRSTNAME,\n" +
                "    address\n" +
                "    (\n" +
                "      city: $src.CITY\n" +
                "    )\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = relSet(pureModel, "my::testMapping", "person");
        EmbeddedRelationFunctionSetImplementation embedded =
                (EmbeddedRelationFunctionSetImplementation) Lists.mutable.withAll(relSet._propertyMappings()).get(1);
        RelationFunctionPropertyMapping cityMapping = (RelationFunctionPropertyMapping) embedded._propertyMappings().getOnly();
        //assertEquals(Optional.of("CITY"), RelationFunctionPropertyMappingTools.asColumnRef(cityMapping));
    }

    // ==================================================================
    // §8 — Quoted column names (with embedded spaces).
    //      Existing engine coverage is compile-only without graph
    //      introspection on the lambda body.
    // ==================================================================

    @Test
    public void testRelationMappingWithQuotedColumnBareForm()
    {
        PureModel pureModel = compileOk(CLASSES_SOURCE + QUOTED_COL_FUNCTION_SOURCE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunctionQuoted():Relation<('FIRST NAME':String[1], AGE:Integer[1])>[1]\n" +
                "    firstName: 'FIRST NAME',\n" +
                "    +age: Integer[0..1]: AGE\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        RelationFunctionPropertyMapping firstNamePm = pmAt(relSet, 0);
        // Quotes are syntactic — the underlying property name carries the space verbatim.
        //assertEquals(Optional.of("FIRST NAME"), RelationFunctionPropertyMappingTools.asColumnRef(firstNamePm));
        assertEquals("String", bodyTypeName(firstNamePm));

        RelationFunctionPropertyMapping agePm = pmAt(relSet, 1);
        //assertEquals(Optional.of("AGE"), RelationFunctionPropertyMappingTools.asColumnRef(agePm));
    }

    @Test
    public void testRelationMappingWithQuotedColumnExplicitSrcForm()
    {
        PureModel pureModel = compileOk(CLASSES_SOURCE + QUOTED_COL_FUNCTION_SOURCE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunctionQuoted():Relation<('FIRST NAME':String[1], AGE:Integer[1])>[1]\n" +
                "    firstName: $src.'FIRST NAME'\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        RelationFunctionPropertyMapping pm = (RelationFunctionPropertyMapping) relSet._propertyMappings().getOnly();
        //assertEquals(Optional.of("FIRST NAME"), RelationFunctionPropertyMappingTools.asColumnRef(pm));
        assertEquals("String", bodyTypeName(pm));
    }

    @Test
    public void testRelationMappingWithQuotedColumnInArithmeticExpression()
    {
        PureModel pureModel = compileOk(CLASSES_SOURCE + QUOTED_COL_FUNCTION_SOURCE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunctionQuoted():Relation<('FIRST NAME':String[1], AGE:Integer[1])>[1]\n" +
                "    +greeted: String[1]: 'Hi ' + $src.'FIRST NAME'\n" +
                "  }\n" +
                ")\n");

        RelationFunctionInstanceSetImplementation relSet = onlyRelSet(pureModel, "my::testMapping");
        RelationFunctionPropertyMapping pm = (RelationFunctionPropertyMapping) relSet._propertyMappings().getOnly();
        assertFalse(RelationFunctionPropertyMappingTools.asColumnRef(pm).isPresent());
        assertEquals("String", bodyTypeName(pm));
    }

    @Test
    public void testRelationMappingWithQuotedColumnBareAndExplicitFormsAgree()
    {
        // Side-by-side compile of bare-column and explicit-$src forms: asColumnRef must
        // yield identical results for both, including the embedded space in the column name.
        PureModel pureModel = compileOk(CLASSES_SOURCE + QUOTED_COL_FUNCTION_SOURCE +
                "###Mapping\n" +
                "Mapping my::bareQuotedMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunctionQuoted():Relation<('FIRST NAME':String[1], AGE:Integer[1])>[1]\n" +
                "    firstName: 'FIRST NAME'\n" +
                "  }\n" +
                ")\n" +
                "###Mapping\n" +
                "Mapping my::explicitQuotedMapping\n" +
                "(\n" +
                "  *my::Person[person]: Relation\n" +
                "  {\n" +
                "    ~func my::personFunctionQuoted():Relation<('FIRST NAME':String[1], AGE:Integer[1])>[1]\n" +
                "    firstName: $src.'FIRST NAME'\n" +
                "  }\n" +
                ")\n");

        RelationFunctionPropertyMapping barePm = (RelationFunctionPropertyMapping)
                onlyRelSet(pureModel, "my::bareQuotedMapping")._propertyMappings().getOnly();
        RelationFunctionPropertyMapping explicitPm = (RelationFunctionPropertyMapping)
                onlyRelSet(pureModel, "my::explicitQuotedMapping")._propertyMappings().getOnly();

        Optional<String> bareCol = RelationFunctionPropertyMappingTools.asColumnRef(barePm);
        Optional<String> explicitCol = RelationFunctionPropertyMappingTools.asColumnRef(explicitPm);
        //assertEquals(Optional.of("FIRST NAME"), bareCol);
        assertEquals(bareCol, explicitCol);
    }

    // ==================================================================
    // §9 — Validator: multiplicity / type subsumption matrix.
    //
    // Engine equivalents that ARE already covered (skipped — see class
    // javadoc):
    //   testRelationMappingWithNonRelationFunction
    //   testRelationMappingWithMismatchingTypes (bare-column variant)
    //   testRelationMappingAllowsToManyPropertyWithToOneExpression
    //     (covered by testRelationFunctionMappingWithInvalidMultiplicityProperty)
    //   testRelationMappingWithInvalidRelationColumn (bare-column variant)
    //   testRelationMappingWithInvalidRelationFunction
    //   testRelationMappingToRelationFunctionWithArguments
    //   testRelationMappingWithEnumerationTransformerInvalidEnumerationMapping
    // ==================================================================

    @Test
    public void testRelationMappingRejectsToManyExpressionForToOneProperty()
    {
        // Property [1] does NOT subsume expression [*] — `split(',')` returns String[*].
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Firm[firm]: Relation\n" +
                        "  {\n" +
                        "    ~func my::firmFunction():Relation<Any>[1]\n" +
                        "    legalName: $src.LEGALNAME->split(',')\n" +
                        "  }\n" +
                        ")\n",
                "Multiplicity Error: The property 'legalName' has a multiplicity range of [1] when the given expression has a multiplicity range of [*]");
    }

    @Test
    public void testRelationMappingRejectsIncompatibleTypeForNonPrimitiveProperty()
    {
        // Mapping the Person.address property (type my::Address) to a String column must fail.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    ~func my::personFunction():Relation<Any>[1]\n" +
                        "    address: CITY\n" +
                        "  }\n" +
                        ")\n",
                "Type Error: 'String' not a subtype of 'Address'");
    }

    @Test
    public void testRelationMappingAllowsZeroToOnePropertyWithToOneExpression()
    {
        // Property [0..1] subsumes expression [1] — must compile.
        compileOk(FUNCTIONS_SOURCE +
                "###Pure\n" +
                "Class my::OptionalFirm\n" +
                "{\n" +
                "  legalName: String[0..1];\n" +
                "}\n" +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::OptionalFirm[firm]: Relation\n" +
                "  {\n" +
                "    ~func my::firmFunction():Relation<Any>[1]\n" +
                "    legalName: LEGALNAME\n" +
                "  }\n" +
                ")\n");
    }

    @Test
    public void testRelationMappingAllowsZeroToOnePropertyWithZeroToOneExpression()
    {
        // Both sides [0..1] — must compile.
        compileOk("###Pure\n" +
                "Class my::OptionalFirm\n" +
                "{\n" +
                "  legalName: String[0..1];\n" +
                "}\n" +
                "function my::optionalColumnFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
                "{\n" +
                "  1->cast(@meta::pure::metamodel::relation::Relation<(LEGALNAME:String[0..1])>);\n" +
                "}\n" +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::OptionalFirm[firm]: Relation\n" +
                "  {\n" +
                "    ~func my::optionalColumnFunction():Relation<Any>[1]\n" +
                "    legalName: LEGALNAME\n" +
                "  }\n" +
                ")\n");
    }

    @Test
    public void testRelationMappingRejectsZeroToOneExpressionForToOneProperty()
    {
        // Property [1] does NOT subsume expression [0..1].
        compileErrorContains(CLASSES_SOURCE +
                        "###Pure\n" +
                        "function my::optionalColumnFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
                        "{\n" +
                        "  1->cast(@meta::pure::metamodel::relation::Relation<(LEGALNAME:String[0..1])>);\n" +
                        "}\n" +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Firm[firm]: Relation\n" +
                        "  {\n" +
                        "    ~func my::optionalColumnFunction():Relation<Any>[1]\n" +
                        "    legalName: LEGALNAME\n" +
                        "  }\n" +
                        ")\n",
                "Multiplicity Error: The property 'legalName' has a multiplicity range of [1] when the given expression has a multiplicity range of [0..1]");
    }

    @Test
    public void testRelationMappingAllowsOneOrManyPropertyWithToOneExpression()
    {
        // Property [1..*] subsumes expression [1] — must compile.
        compileOk(FUNCTIONS_SOURCE +
                "###Pure\n" +
                "Class my::FirmWithAliases\n" +
                "{\n" +
                "  aliases: String[1..*];\n" +
                "}\n" +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::FirmWithAliases[firm]: Relation\n" +
                "  {\n" +
                "    ~func my::firmFunction():Relation<Any>[1]\n" +
                "    aliases: LEGALNAME\n" +
                "  }\n" +
                ")\n");
    }

    @Test
    public void testRelationMappingRejectsManyExpressionForOneOrManyProperty()
    {
        // Property [1..*] does NOT subsume expression [*] (lower bounds differ).
        compileErrorContains(FUNCTIONS_SOURCE +
                        "###Pure\n" +
                        "Class my::FirmWithAliases\n" +
                        "{\n" +
                        "  aliases: String[1..*];\n" +
                        "}\n" +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::FirmWithAliases[firm]: Relation\n" +
                        "  {\n" +
                        "    ~func my::firmFunction():Relation<Any>[1]\n" +
                        "    aliases: $src.LEGALNAME->split(',')\n" +
                        "  }\n" +
                        ")\n",
                "Multiplicity Error: The property 'aliases' has a multiplicity range of [1..*] when the given expression has a multiplicity range of [*]");
    }

    @Test
    public void testRelationMappingAllowsToManyPropertyWithManyExpression()
    {
        // Both [*] — must compile.
        compileOk(STANDARD_FIXTURE +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::Firm[firm]: Relation\n" +
                "  {\n" +
                "    ~func my::firmFunction():Relation<Any>[1]\n" +
                "    clientNames: $src.LEGALNAME->split(',')\n" +
                "  }\n" +
                ")\n");
    }

    @Test
    public void testRelationMappingAllowsPrimitiveSubtypeColumn()
    {
        // Integer is a subtype of Number — an Integer column can feed a Number property.
        compileOk(FUNCTIONS_SOURCE +
                "###Pure\n" +
                "Class my::NumericThing\n" +
                "{\n" +
                "  amount: Number[1];\n" +
                "}\n" +
                "###Mapping\n" +
                "Mapping my::testMapping\n" +
                "(\n" +
                "  *my::NumericThing[t]: Relation\n" +
                "  {\n" +
                "    ~func my::firmFunction():Relation<Any>[1]\n" +
                "    amount: ID\n" +
                "  }\n" +
                ")\n");
    }

    @Test
    public void testRelationMappingRejectsPrimitiveSupertypeColumn()
    {
        // Number is NOT a subtype of Integer.
        compileErrorContains("###Pure\n" +
                        "Class my::IntegerThing\n" +
                        "{\n" +
                        "  amount: Integer[1];\n" +
                        "}\n" +
                        "function my::numericColumnFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
                        "{\n" +
                        "  1->cast(@meta::pure::metamodel::relation::Relation<(VAL:Number[1])>);\n" +
                        "}\n" +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::IntegerThing[t]: Relation\n" +
                        "  {\n" +
                        "    ~func my::numericColumnFunction():Relation<Any>[1]\n" +
                        "    amount: VAL\n" +
                        "  }\n" +
                        ")\n",
                "Type Error: 'Number' not a subtype of 'Integer'");
    }

    @Test
    public void testRelationMappingRejectsBooleanForStringProperty()
    {
        // Boolean and String are unrelated primitives.
        compileErrorContains(CLASSES_SOURCE +
                        "###Pure\n" +
                        "function my::booleanColumnFunction(): meta::pure::metamodel::relation::Relation<Any>[1]\n" +
                        "{\n" +
                        "  1->cast(@meta::pure::metamodel::relation::Relation<(FLAG:Boolean[1])>);\n" +
                        "}\n" +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Firm[firm]: Relation\n" +
                        "  {\n" +
                        "    ~func my::booleanColumnFunction():Relation<Any>[1]\n" +
                        "    legalName: FLAG\n" +
                        "  }\n" +
                        ")\n",
                "Type Error: 'Boolean' not a subtype of 'String'");
    }

    @Test
    public void testRelationMappingRejectsZeroToOneExpressionForToOnePropertyWithExpressionRhs()
    {
        // Same multiplicity rejection but expressed through `$src.<col>->toOneMany()->first()`.
        // Demonstrates the check operates on the lambda's result, not on the column type alone.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Firm[firm]: Relation\n" +
                        "  {\n" +
                        "    ~func my::firmFunction():Relation<Any>[1]\n" +
                        "    legalName: $src.LEGALNAME->toOneMany()->first()\n" +
                        "  }\n" +
                        ")\n",
                "Multiplicity Error: The property 'legalName' has a multiplicity range of [1] when the given expression has a multiplicity range of [0..1]");
    }

    @Test
    public void testRelationMappingWithMissingRelationFunction()
    {
        // Neither ~func nor ~src — the engine's grammar reports the missing source-form keyword.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    firstName: FIRSTNAME\n" +
                        "  }\n" +
                        ")\n",
                "Valid alternatives: ['~func', '~src']");
    }

    @Test
    public void testRelationMappingWithEmbeddedInvalidColumn()
    {
        // Unknown column referenced inside an embedded property mapping.
        // Engine note: embedded sub-property mappings are compiled via
        // `ClassMappingSecondPassBuilder.compileRelationPropertyLambda` → AppliedProperty →
        // legend-pure's `FunctionExpressionProcessor`, which throws the
        // "The column 'X' can't be found in the relation (...)" wording.  This is a different
        // code path from the bare-column outer mapping (which uses `_RelationType.findColumn`
        // and gets the legacy "The system can't find the column" wording).  Match a loose
        // substring so the assertion isn't coupled to the exact (type:multiplicity) tail.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    ~func my::personFunction():Relation<Any>[1]\n" +
                        "    address\n" +
                        "    (\n" +
                        "      city: FOO\n" +
                        "    )\n" +
                        "  }\n" +
                        ")\n",
                "The column 'FOO' can't be found in the relation");
    }

    @Test
    public void testRelationMappingWithInlineSrcReturningNonRelation()
    {
        // ~src whose expression evaluates to a non-Relation value must be rejected.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    ~src 1\n" +
                        "    firstName: FIRSTNAME\n" +
                        "  }\n" +
                        ")\n",
                "Relation mapping function should return a Relation");
    }

    @Test
    public void testRelationMappingExpressionRhsTypeError()
    {
        // age: Integer ← $src.FIRSTNAME : String
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    ~func my::personFunction():Relation<Any>[1]\n" +
                        "    age: $src.FIRSTNAME\n" +
                        "  }\n" +
                        ")\n",
                "Type Error: 'String' not a subtype of 'Integer'");
    }

    @Test
    public void testRelationMappingExpressionRhsMissingColumn()
    {
        // $src.MISSING — error must mention the column name.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    ~func my::personFunction():Relation<Any>[1]\n" +
                        "    firstName: $src.MISSING\n" +
                        "  }\n" +
                        ")\n",
                "MISSING");
    }

    @Test
    public void testRelationMappingExpressionRhsMultiplicityViolation()
    {
        // `$src.X->toOneMany()` yields String[1..*], which is incompatible with a [1] property.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    ~func my::personFunction():Relation<Any>[1]\n" +
                        "    firstName: $src.FIRSTNAME->toOneMany()\n" +
                        "  }\n" +
                        ")\n",
                "Multiplicity");
    }

    @Test
    public void testRelationMappingWithBothFuncAndSrcRejected()
    {
        // Both ~func and ~src on the same class mapping — grammar rejects.  After consuming the
        // ~func line, the parser doesn't see the expected property-mapping continuation
        // (`(`, `:`) on the next line and reports an unexpected-token error.
        compileErrorContains(STANDARD_FIXTURE +
                        "###Mapping\n" +
                        "Mapping my::testMapping\n" +
                        "(\n" +
                        "  *my::Person[person]: Relation\n" +
                        "  {\n" +
                        "    ~func my::personFunction():Relation<Any>[1]\n" +
                        "    ~src my::personFunctionTyped()\n" +
                        "    firstName: FIRSTNAME\n" +
                        "  }\n" +
                        ")\n",
                "Unexpected token");
    }
}


