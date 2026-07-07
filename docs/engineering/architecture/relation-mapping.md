# Relation Mappings (`~func` / `~src`)

> **Audience.** Engine developers working on class-to-relation mappings
> (`RelationFunctionInstanceSetImplementation`): grammar, compiler, SQL generator,
> and routing layer. This document covers the full feature set — from the simplest
> primitive-column mapping through primary keys, local properties, binding
> (semi-structured), enumeration, embedded, inline embedded, union, and the
> variant / lift path that synthesises semi-structured mappings from a `valueFn`
> lambda body.
>
> **Authoritative sources** (referenced throughout):
>
> | Stage | File |
> |-------|------|
> | Lexer grammar | `legend-engine-language-pure-grammar/.../antlr4/mapping/relationFunctionMapping/RelationFunctionMappingLexerGrammar.g4` |
> | Parser grammar | `legend-engine-language-pure-grammar/.../antlr4/mapping/relationFunctionMapping/RelationFunctionMappingParserGrammar.g4` |
> | Parse-tree walker | `legend-engine-language-pure-grammar/.../mapping/RelationFunctionMappingParseTreeWalker.java` |
> | Grammar entry point | `legend-engine-language-pure-grammar/.../CorePureGrammarParser.java` (`parseRelationFunctionClassMapping`) |
> | Composer | `legend-engine-language-pure-grammar/.../to/DEPRECATED_PureGrammarComposerCore.java` |
> | Protocol — class mapping POJO | `legend-engine-protocol-pure/.../mapping/relationFunction/RelationFunctionClassMapping.java` |
> | Protocol — property mapping POJO | `legend-engine-protocol-pure/.../mapping/relationFunction/RelationFunctionPropertyMapping.java` |
> | Protocol — embedded POJO | `legend-engine-protocol-pure/.../mapping/relationFunction/RelationFunctionEmbeddedPropertyMapping.java` |
> | Compiler — prerequisite pass | `legend-engine-language-pure-compiler/.../toPureGraph/ClassMappingPrerequisiteElementsPassBuilder.java` |
> | Compiler — first pass | `legend-engine-language-pure-compiler/.../toPureGraph/ClassMappingFirstPassBuilder.java` |
> | Compiler — second pass | `legend-engine-language-pure-compiler/.../toPureGraph/ClassMappingSecondPassBuilder.java` |
> | Compiler — third pass | `legend-engine-language-pure-compiler/.../toPureGraph/ClassMappingThirdPassBuilder.java` |
> | Compiler — property mappings | `legend-engine-language-pure-compiler/.../toPureGraph/PropertyMappingBuilder.java` |
> | Compiler — bare-column fast-path helper | `legend-engine-language-pure-compiler/.../toPureGraph/RelationFunctionPropertyMappingTools.java` |
> | Compiler — validation | `legend-engine-language-pure-compiler/.../toPureGraph/validator/MappingValidator.java` |
> | Primary-key inference (Pure) | `legend-engine-pure-code-compiled-core/.../core/pure/mapping/relationFunctionMapping.pure` |
> | Helper functions (Pure) | `core_relational/relational/helperFunctions/helperFunctions.pure` |
> | SQL metamodel | `core_relational/relational/pureToSQLQuery/metamodel.pure` |
> | SQL generation — main | `core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure` (`processRelationFunctionClassMapping`, `transformRelationFunctionPropertyMappingToRelational`) |
> | SQL generation — variant / semi-structured | `core_relational/relational/pureToSQLQuery/pureToSQLQuery_variant.pure` |
> | SQL generation — union | `core_relational/relational/pureToSQLQuery/pureToSQLQuery_union.pure` |
> | Router — store contract | `core/pure/router/store/cluster.pure` (`storeContractForSetImplementation`) |
> | Router — set routing | `core/pure/router/store/routing.pure` (`potentiallyRouteRelationFunctionSets`) |
> | Router — inline embedded resolution | `core/pure/mapping/mappingExtension.pure` (`inlineEmbeddedRelationFunctionMapping`) |
> | Protocol transfer | `legend-engine-pure-code-compiled-core/.../core/pure/protocol/vX_X_X/transfers/mapping.pure` |

---

## 0. What is a Relation Mapping?

A **Relation mapping** (`Relation` keyword in mapping grammar) lets you map a Pure
class to the output of a Pure `Relation<Any>` expression. Two source forms are
supported inside the mapping block:

- `~func <descriptor>` — reference an existing Pure `FunctionDefinition` by path
  or descriptor.
- `~src <expression>` — inline a zero-arg expression that evaluates to a
  `Relation<Any>`. The parser wraps it in a synthetic `LambdaFunction`; the
  compiler resolves its row type the same way it resolves a `~func` reference.

Properties bind to columns of the relation function's typed `RelationType`. Two
RHS forms are supported per property:

- Bare column identifier (`propName: FIRSTNAME`) — legacy sugar. The compiler
  lowers it to `{$src.FIRSTNAME}` so a single downstream code path handles
  everything.
- Pure expression over `$src` (`propName: $src.FIRSTNAME + '-' + $src.LASTNAME`)
  — arbitrary lambda body typed at the relation function's row type.

The primary key can be declared explicitly with `~primaryKey`, or inferred at
plan-generation time from the relation function's body (see §8).

Compared with the classic `Relational` mapping (which is tightly coupled to a
physical schema via `~mainTable`, join graphs, and `[db]Table.Column` paths), a
Relation mapping:

- uses a **Pure function** (or inline expression) as its data source — the
  source is compiled and type-checked like any other Pure code;
- binds properties by column name or lambda body, not by physical table path;
- feeds SQL generation through `processRelationFunctionClassMapping`, which
  evaluates the function body and wraps it in a sub-select rather than
  referencing a physical table directly.

---

## 1. Complete Grammar Reference

### 1.1 Lexer tokens

```antlr
RELATION_FUNC:        '~func' ;
RELATION_SRC:         '~src' ;
RELATION_PRIMARY_KEY: '~primaryKey' ;
BINDING:              'Binding' ;
ENUMERATION_MAPPING:  'EnumerationMapping' ;
INLINE:               'Inline' ;
```

All six tokens are introduced by `RelationFunctionMappingLexerGrammar.g4` and are
imported into the main M3 lexer hierarchy.

### 1.2 Parser rules

```antlr
relationFunctionMapping:
    relationSource
    primaryKey?
    (singlePropertyMapping (COMMA singlePropertyMapping)*)?
    EOF
;

// ~func references an existing Pure function by descriptor or qualified name.
// ~src takes an inline zero-arg Pure expression that evaluates to a Relation —
// the walker wraps it in a synthetic `{ <expr>}` lambda so the rest of the
// pipeline can treat both forms uniformly.
relationSource:
    RELATION_FUNC functionIdentifier
  | RELATION_SRC  combinedExpression
;

primaryKey:
    RELATION_PRIMARY_KEY COLON
    (identifier | BRACKET_OPEN identifier (COMMA identifier)* BRACKET_CLOSE)
;

singlePropertyMapping:
    singleLocalPropertyMapping | singleNonLocalPropertyMapping
;

// Local (derived) property — adds a new property to the class in the mapping scope
singleLocalPropertyMapping:
    PLUS qualifiedName COLON type multiplicity relationFunctionPropertyMapping
;

// Standard property mapping
singleNonLocalPropertyMapping:
    qualifiedName
    (
        relationFunctionPropertyMapping
      | relationFunctionEmbeddedPropertyMapping
      | inlineRelationFunctionEmbeddedPropertyMapping
    )
;

// Property RHS: bare `columnName` (legacy sugar; lowered to `$src.<col>`) or a
// full Pure expression over `$src`. The bare-column form is matched by
// `identifier` alone; anything more complex (starting with `$`, containing
// operators / function calls) falls through to combinedExpression.
relationFunctionPropertyMapping:
    COLON (transformer)? (identifier | combinedExpression)
;

transformer:
    bindingTransformer | enumTransformer
;

bindingTransformer:
    BINDING qualifiedName COLON
;

enumTransformer:
    ENUMERATION_MAPPING identifier COLON
;

// Normal embedded — child columns come from the same relation function
relationFunctionEmbeddedPropertyMapping:
    PAREN_OPEN
    (singlePropertyMapping (COMMA singlePropertyMapping)*)?
    PAREN_CLOSE
;

// Inline embedded — delegates to a separately-declared class mapping
inlineRelationFunctionEmbeddedPropertyMapping:
    PAREN_OPEN PAREN_CLOSE INLINE BRACKET_OPEN identifier BRACKET_CLOSE
;
```

The `Relation` block-type keyword is registered in `CorePureGrammarParser` as
`RELATION_EXPRESSION` and dispatched to `parseRelationFunctionClassMapping`.

### 1.3 Full grammar skeleton

```
###Mapping
Mapping myPkg::MyMapping
(
  // Root class mapping — asterisk makes this the default mapping for the class
  *MyClass[optionalId]: Relation
  {
    ~func      myPkg::myFunction():Relation<Any>[1]      // OR ~src <expression>
    ~primaryKey: [colA, colB]                            // optional; inferred if omitted

    // Bare column (legacy sugar) — lowered to `{$src.COLUMN_NAME}` at SecondPass
    primitiveProperty  : COLUMN_NAME

    // Pure expression over $src — compiled as a $src-parameterised lambda
    derivedProperty    : $src.FIRST_NAME + ' ' + $src.LAST_NAME

    enumProperty       : EnumerationMapping myEnumMapping : ENUM_COLUMN

    // Semi-structured (binary / JSON) property backed by a binding
    complexProperty    : Binding myPkg::MyBinding : SEMI_STRUCT_COLUMN

    // Normal embedded sub-object (child columns come from the same relation)
    subObject
    (
      childProp1: CHILD_COL_1,
      childProp2: CHILD_COL_2
    )

    // Inline embedded — delegates to separately-declared addressSet mapping
    subObject2 () Inline [addressSet]

    // Local property (adds a transient property to the class in this mapping scope)
    +localProp: String[1] : LOCAL_COL
  }
)
```

---

## 2. Examples

### 2.1 Primitive columns with `~func` (minimal)

```
###Pure
Class myPkg::Person
{
  firstName: String[1];
  age:       Integer[1];
}

function myPkg::personFunc(): Relation<(FIRSTNAME:String, AGE:Integer)>[1]
{
  #>{myDb.PERSON}#->select(~[FIRSTNAME, AGE])
}

###Mapping
Mapping myPkg::PersonMapping
(
  *Person: Relation
  {
    ~func myPkg::personFunc():Relation<Any>[1]
    firstName: FIRSTNAME,
    age:       AGE
  }
)
```

A query `Person.all()->filter(x | $x.age > 30)` routes through
`processRelationFunctionClassMapping`, evaluates the function body to a
`SelectSQLQuery`, wraps it in a sub-select, then applies the filter on top.

### 2.2 Inline expression source (`~src`)

```
*Person: Relation
{
  ~src #>{myDb.PERSON}#->select(~[FIRSTNAME, AGE])
  firstName: FIRSTNAME,
  age:       AGE
}
```

The parser wraps `#>{...}#->select(...)` in a synthetic zero-arg
`LambdaFunction`. The compiler resolves its row type identically to the `~func`
path.

### 2.3 Explicit primary key

```
*Person: Relation
{
  ~func      myPkg::personFunc():Relation<Any>[1]
  ~primaryKey: ID
  firstName: FIRSTNAME,
  age:       AGE
}
```

Multiple PK columns:

```
~primaryKey: [FIRST_NAME, LAST_NAME]
```

If `~primaryKey` is absent, the runtime infers PK columns by walking the
function body — see §8.

### 2.4 Property RHS as a Pure expression

```
*Person: Relation
{
  ~func myPkg::personFunc():Relation<Any>[1]
  firstName: $src.'FIRST NAME',      // equivalent to bare `firstName: 'FIRST NAME'`
  ageInMonths: $src.AGE * 12,        // arithmetic — DynaFunction
  greeting: 'Hello ' + $src.'FIRST NAME'
}
```

Each RHS is stored as a `valueFn` `LambdaFunction` whose only parameter is
`$src`, typed at the relation function's row type. Bare-column RHSs are lowered
to `{$src.<col>}` at SecondPass so downstream consumers see a single shape.

### 2.5 Local (derived) property

Local properties extend the class within the mapping scope without modifying the
canonical Pure class definition:

```
*Person: Relation
{
  ~func myPkg::personFunc():Relation<Any>[1]
  firstName:   FIRSTNAME,
  +displayAge: String[1]: AGE_DISPLAY   // adds displayAge: String[1] to Person in this scope
}
```

### 2.6 Semi-structured column (binding transformer)

Complex JSON / binary columns are mapped via a `Binding`:

```
*Person: Relation
{
  ~func myPkg::personFunc():Relation<Any>[1]
  firstName: FIRSTNAME,
  address:   Binding myPkg::AddressBinding : ADDRESS_JSON
}
```

The compiler checks that the binding's model unit includes the property's return
type. At SQL generation time the RFPM is turned into a
`SemiStructuredEmbeddedRelationalInstanceSetImplementation` whose relational
operation element is the placeholder-TAC relop produced by evaluating
`{$src.ADDRESS_JSON}` against the synthetic RF cursor. The
`BindingTransformer` flows through unchanged.

### 2.7 Enumeration mapping

Map an enum-typed property to a relation column, converting raw string values:

```
###Pure
Enum myPkg::EmployeeType { CONTRACT; FULL_TIME; }

Class myPkg::Employee
{
  name:         String[1];
  employeeType: myPkg::EmployeeType[1];
}

###Mapping
Mapping myPkg::EmployeeMapping
(
  *Employee: Relation
  {
    ~func myPkg::employeeFunc():Relation<Any>[1]
    name:         NAME,
    employeeType: EnumerationMapping empTypeMap : EMP_TYPE
  }

  EmployeeType: EnumerationMapping empTypeMap
  {
    CONTRACT:  'CONTRACT',
    FULL_TIME: ['SALARY', 'FULL_TIME']
  }
)
```

### 2.8 Normal embedded mapping

Map a sub-object whose columns come from the same relation:

```
###Pure
Class myPkg::Address     { street: String[1]; city: String[1]; }
Class myPkg::PersonWithAddress
{
  firstName: String[1];
  address:   myPkg::Address[1];
}

###Mapping
Mapping myPkg::EmbeddedMapping
(
  *PersonWithAddress: Relation
  {
    ~func myPkg::personFunc():Relation<Any>[1]
    firstName: FIRSTNAME,
    address
    (
      street: STREET,
      city:   CITY
    )
  }
)
```

### 2.9 Inline embedded mapping

Delegate the sub-object mapping to a separately-declared class mapping (which
may use a different relation function):

```
*PersonWithAddress[personSet]: Relation
{
  ~func myPkg::personFunc():Relation<Any>[1]
  firstName: FIRSTNAME,
  address () Inline [addressSet]
}

*Address[addressSet]: Relation
{
  ~func myPkg::personFunc():Relation<Any>[1]
  street: STREET,
  city:   CITY
}
```

### 2.10 Union mapping

Two (or more) Relation class mappings unioned together:

```
*Person: Operation
{
  meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(
    rfSet1, rfSet2
  )
}

*Person[rfSet1]: Relation
{
  ~func myPkg::personSet1Func():Relation<Any>[1]
  lastName: $src.lastName_s1->toOne()
}

*Person[rfSet2]: Relation
{
  ~func myPkg::personSet2Func():Relation<Any>[1]
  lastName: $src.lastName_s2->toOne()
}
```

Mixed union (one Relation leaf, one Relational leaf):

```
*Person: Operation
{
  meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(
    rfSet1, relSet2
  )
}
*Person[rfSet1]: Relation     { ~func myPkg::f():Relation<Any>[1] ... }
*Person[relSet2]: Relational  { ~mainTable [db]PERSON ... }
```

All leaves must resolve to the **same store**.

---

## 3. Pipeline at a Glance

```
   Mapping grammar text
         │
         ▼  (1) Parser  [RelationFunctionMappingParseTreeWalker]
   Protocol POJOs
     ├── RelationFunctionClassMapping
     │     ├── relationFunction : PackageableElementPointer  (set for ~func)
     │     │                       OR
     │     ├── sourceLambda     : LambdaFunction             (set for ~src)
     │     ├── primaryKey       : List<String>
     │     └── propertyMappings : List<PropertyMapping>
     │           ├── RelationFunctionPropertyMapping
     │           │     ├── column   (bare identifier RHS)
     │           │     │     OR
     │           │     ├── valueFn  (combinedExpression RHS)
     │           │     └── bindingTransformer? / enumMappingId?
     │           └── RelationFunctionEmbeddedPropertyMapping
     │                 ├── id / setImplementationId  (inline form)
     │                 └── propertyMappings          (normal form, recursive)
         │
         ▼  (2) Compiler — 4 passes
   Pure graph objects
     ├── (Prerequisite) declares CLASS + FUNCTION prerequisites so the function
     │                  is compiled before the mapping (needed for row-type
     │                  extraction and PK resolution)
     ├── (First)        creates RelationFunctionInstanceSetImplementation and
     │                  compiles skeleton property mappings via PropertyMappingBuilder
     ├── (Second)       resolves ~func / compiles ~src → FunctionDefinition;
     │                  validates return type is Relation<...>;
     │                  extracts row type from last-expression's RelationType;
     │                  synthesises _valueFn lambda for each property mapping
     │                  (bare-column form lowered to `{$src.<col>}`);
     │                  propagates relationFunction into embedded sets
     └── (Third)        if ~primaryKey set, resolves column names against the
                        function's RelationType (hard error if unknown column)
         │
         ▼  (3) Validation  [MappingValidator]
     ├── inline embedded: id must exist as a class-mapping ID in the same Mapping
     └── each RelationFunctionInstanceSetImplementation:
           ├── relation function has no parameters
           ├── return type is Relation<...>
           └── each RelationFunctionPropertyMapping._valueFn body:
                  multiplicity subsumed by property multiplicity, and body raw
                  type is subtype of property raw type
                  (subtype check skipped when transformer is present)
         │
         ▼  (4) Routing  [cluster.pure, routing.pure]
   Store contract resolved per set:
     ├── RelationFunctionInstanceSetImplementation → store from routed function
     ├── EmbeddedSetImplementation                 → delegate to owner
     └── OperationSetImplementation (union)        → resolve per leaf; enforce single store
   Class-level routing populates the routed function into each set impl and
   caches routed sets in classMappingsByClass.
         │
         ▼  (5) SQL Generation  [pureToSQLQuery.pure, pureToSQLQuery_variant.pure,
         │                       pureToSQLQuery_union.pure]
   processRelationFunctionClassMapping
     ├── auto-infer primaryKey if empty (resolveRelationFunctionPrimaryKey)
     ├── route function (potentiallyRouteRelationFunctionSet)
     ├── evaluate body → SelectSQLQuery
     └── moveSelectQueryToSubSelect → wrapped sub-select

   Per property navigation:
     transformRelationFunctionPropertyMappingToRelational (RFPM → RPM):
       ├── build synthetic RF cursor with placeholder RelationFunctionColumn TACs
       ├── bind $src to the synthetic cursor and evaluate valueFn body
       ├── detect variant-ness (expressionTouchesVariant)
       └── synthesise downstream PM:
             ├── Binding                    → SemiStructuredEmbeddedRelationalInstanceSetImplementation
             ├── non-variant valueFn        → RelationalPropertyMapping
             ├── variant + Class target     → SemiStructuredEmbeddedRelationalInstanceSetImplementation
             └── variant + primitive target → SemiStructuredRelationalPropertyMapping
   Placeholder TACs at the leaves are resolved against the real source operation
   at column-navigation time by resolveTableAliasColumn.
```

---

## 4. Parser

### 4.1 Entry point

`CorePureGrammarParser` registers `"Relation"` as a mapping-block keyword and
dispatches to `parseRelationFunctionClassMapping`, which:

1. Reads the block header (`*ClassName[id]`, `root`, `extendsClassMappingId`).
2. Invokes `RelationFunctionMappingParseTreeWalker.visitRelationFunctionClassMapping`.

### 4.2 Walker: class-level source

```java
// RelationFunctionMappingParseTreeWalker.visitRelationFunctionClassMapping
if (sourceCtx.RELATION_FUNC() != null)
{
    relationFunctionClassMapping.relationFunction =
        new PackageableElementPointer(FUNCTION, sourceCtx.functionIdentifier().getText(), ...);
}
else
{
    // ~src <combinedExpression> — wrap the inline expression in a zero-arg
    // lambda so the downstream compiler can resolve the row type uniformly
    // with the ~func path.
    relationFunctionClassMapping.sourceLambda =
        visitInlineExpressionAsLambda(sourceCtx.combinedExpression());
}

relationFunctionClassMapping.primaryKey =
    ctx.primaryKey() != null
        ? ctx.primaryKey().identifier().stream()
             .map(PureGrammarParserUtility::fromIdentifier)
             .collect(Collectors.toList())
        : Collections.emptyList();

relationFunctionClassMapping.propertyMappings =
    ctx.singlePropertyMapping().stream()
       .map(c -> this.visitPropertyMapping(c, ...))
       .collect(Collectors.toList());
```

`visitInlineExpressionAsLambda` re-parses the raw text of the
`combinedExpression` (preserving offsets) via `DomainParser.parseCombinedExpression`
and returns a `LambdaFunction` with a single-element body and empty parameters.

### 4.3 Walker: property RHS dispatch

`visitPropertyMapping` branches on which child rule is present:

| Branch | Result |
|--------|--------|
| `singleLocalPropertyMapping` | `RelationFunctionPropertyMapping` with `localMappingProperty` set |
| `relationFunctionEmbeddedPropertyMapping` | `RelationFunctionEmbeddedPropertyMapping` (normal, recursive) |
| `inlineRelationFunctionEmbeddedPropertyMapping` | `RelationFunctionEmbeddedPropertyMapping` (inline: `id` set, empty `propertyMappings`) |
| plain `relationFunctionPropertyMapping` | `RelationFunctionPropertyMapping` (column or valueFn RHS) |

Inside `visitRelationFunctionPropertyMapping`:

```java
if (ctx.identifier() != null)
{
    propertyMapping.column = PureGrammarParserUtility.fromIdentifier(ctx.identifier());
}
else if (ctx.combinedExpression() != null)
{
    propertyMapping.valueFn = visitInlineExpressionAsLambda(ctx.combinedExpression());
}
```

A `bindingTransformer` sets `bindingTransformer.binding`; an `enumTransformer`
sets `enumMappingId`.

### 4.4 Protocol shapes after parsing

```
// Class mapping — one of relationFunction or sourceLambda is set
RelationFunctionClassMapping {
  relationFunction : PackageableElementPointer("myPkg::personFunc")   // OR
  sourceLambda     : LambdaFunction { body: [<inline expr VS>], parameters: [] }
  primaryKey       : List<String>
  propertyMappings : [ ... ]
}

// Property mapping — one of column or valueFn is set
RelationFunctionPropertyMapping {
  property           : PropertyPointer("firstName")
  column             : "FIRSTNAME"                              // OR
  valueFn            : LambdaFunction { body: [<user expr VS>], parameters: [] }
  enumMappingId      : null | "empTypeMap"
  bindingTransformer : null | BindingTransformer{ binding: "myPkg::MyBinding" }
  localMappingProperty: null | LocalMappingPropertyInfo{ type, multiplicity }
}

// Normal embedded
RelationFunctionEmbeddedPropertyMapping {
  property         : PropertyPointer("address")
  id               : null
  setImplementationId: null
  propertyMappings : [ RelationFunctionPropertyMapping{"street", column:"STREET"}, ... ]
}

// Inline embedded
RelationFunctionEmbeddedPropertyMapping {
  property         : PropertyPointer("address")
  id               : "addressSet"
  setImplementationId: "addressSet"
  propertyMappings : []
}
```

---

## 5. Compiler

The compiler runs four sequential passes per class mapping:
`ClassMappingPrerequisiteElementsPassBuilder` → `ClassMappingFirstPassBuilder`
→ `ClassMappingSecondPassBuilder` → `ClassMappingThirdPassBuilder`.

### 5.1 Prerequisite pass — `ClassMappingPrerequisiteElementsPassBuilder`

Declares upstream dependencies so the compiler orders compilation correctly:

```java
public Set<PackageableElementPointer> visit(RelationFunctionClassMapping classMapping)
{
    this.prerequisiteElements.add(new PackageableElementPointer(
        PackageableElementType.CLASS, classMapping._class, classMapping.classSourceInformation));

    // Only ~func has an external function reference to declare as prerequisite;
    // ~src carries an inline lambda compiled in-place at SecondPass.
    if (classMapping.relationFunction != null)
    {
        this.prerequisiteElements.add(new PackageableElementPointer(
            PackageableElementType.FUNCTION,
            classMapping.relationFunction.path,
            classMapping.relationFunction.sourceInformation));
    }

    PropertyMappingPrerequisiteElementsBuilder propertyMappingBuilder =
        new PropertyMappingPrerequisiteElementsBuilder(this.context, this.prerequisiteElements);
    ListIterate.forEach(classMapping.propertyMappings, pm -> pm.accept(propertyMappingBuilder));
    return this.prerequisiteElements;
}
```

Making the function a prerequisite guarantees `expressionSequence` is fully
typed before ThirdPass tries to resolve `~primaryKey` names against the row
type.

### 5.2 First pass — `ClassMappingFirstPassBuilder`

Creates the `RelationFunctionInstanceSetImplementation` node and compiles
property-mapping skeletons (the relation function itself is not resolved yet):

```java
public Pair<SetImplementation, RichIterable<EmbeddedSetImplementation>>
visit(RelationFunctionClassMapping classMapping)
{
    final Class<?> pureClass = context.resolveClass(classMapping._class, ...);
    String id = HelperMappingBuilder.getClassMappingId(classMapping, this.context);

    RelationFunctionInstanceSetImplementation setImpl =
        new Root_meta_pure_mapping_relation_RelationFunctionInstanceSetImplementation_Impl(id, ...)
            ._class(pureClass)
            ._id(id)
            ._superSetImplementationId(classMapping.extendsClassMappingId)
            ._root(classMapping.root)
            ._parent(parentMapping)
            ._propertyMappings(ListIterate.collect(classMapping.propertyMappings,
                p -> p.accept(new PropertyMappingBuilder(
                    context, baseSetImpl, HelperMappingBuilder.getAllEnumerationMappings(parentMapping)))));

    HelperMappingBuilder.buildMappingClassOutOfLocalProperties(
        setImpl, setImpl._propertyMappings(), context);

    MutableList<EmbeddedSetImplementation> embeddedSets =
        Lists.mutable.withAll(setImpl._propertyMappings().selectInstancesOf(EmbeddedSetImplementation.class));
    return Tuples.pair(setImpl, embeddedSets);
}
```

### 5.3 Second pass — `ClassMappingSecondPassBuilder`

Resolves the source and builds per-property `_valueFn` lambdas.

**Step 1 — resolve source, attach to `_relationFunction`:**

```java
FunctionDefinition<?> relationFunction;
if (classMapping.relationFunction != null)
{
    // ~func: resolve by descriptor / qualified name
    String functionId = FunctionDescriptor.isValidFunctionDescriptor(functionPath)
                         ? FunctionDescriptor.functionDescriptorToId(functionPath)
                         : functionPath;
    relationFunction = (FunctionDefinition<?>) context.resolvePackageableElement(functionId, ...);
}
else if (classMapping.sourceLambda != null)
{
    // ~src: compile the inline expression as a zero-arg LambdaFunction
    relationFunction = HelperValueSpecificationBuilder.buildLambdaWithContext(
        classMapping.sourceLambda.body,
        classMapping.sourceLambda.parameters == null
            ? Lists.fixedSize.empty()
            : classMapping.sourceLambda.parameters,
        classMapping.sourceLambda.sourceInformation,
        context,
        new ProcessingContext("Building ~src relation source lambda ..."));
}
else
{
    throw new EngineException("Relation class mapping must specify either '~func' or '~src'.", ...);
}
setImpl._relationFunction(relationFunction);
```

**Step 2 — early return-type check** (duplicated authoritative check exists in
`MappingValidator`; the SecondPass check produces a clearer error before per-
property lambda building starts):

```java
if (!processorSupport.type_subTypeOf(
        resolvedFnType._returnType()._rawType(),
        processorSupport.package_getByUserPath(M3Paths.Relation)))
{
    throw new EngineException("Relation mapping function should return a Relation! Found a "
        + GenericType.print(resolvedFnType._returnType(), processorSupport) + " instead.", ...);
}
```

**Step 3 — extract row type** from the function's last-expression `RelationType`:

```java
GenericType lastExprType = relationFunction._expressionSequence().toList().getLast()._genericType();
GenericType srcType = lastExprType._typeArguments().toList().isEmpty()
                        ? null
                        : lastExprType._typeArguments().toList().getFirst();
```

**Step 4 — build `_valueFn` for each property mapping**, recursing into embedded
sets and propagating the relation function down:

```java
private void buildValueFunctionsForPropertyMappings(
    List<PropertyMapping> protocolPropertyMappings,
    PropertyMappingsImplementation parent,
    FunctionDefinition<?> relationFunction,
    GenericType srcType)
{
    ... walk protocol PMs in parallel with M3 PMs ...
    for each RelationFunctionPropertyMapping:
        LambdaFunction<?> valueFn = buildPropertyValueFn(pPm, srcType, parent._id());
        if (valueFn != null) mPm._valueFn(valueFn);

    for each RelationFunctionEmbeddedPropertyMapping:
        mEmb._relationFunction(relationFunction);
        buildValueFunctionsForPropertyMappings(pEmb.propertyMappings, mEmb, relationFunction, srcType);
}
```

`buildPropertyValueFn` picks the AST to compile:

- If `pm.valueFn` is set — use its body verbatim.
- Otherwise (bare column) — synthesise `AppliedProperty` on `$src`:
  ```java
  Variable srcRef = new Variable(); srcRef.name = "src";
  AppliedProperty colAccess = new AppliedProperty();
  colAccess.property = pm.column;
  colAccess.parameters = Collections.singletonList(srcRef);
  body = Collections.singletonList(colAccess);
  ```
- Returns `null` when neither is set — the property mapping is invalid and will
  be caught by `MappingValidator`.

`compileRelationPropertyLambda` builds the lambda with a `src`-typed
`VariableExpression`, evaluating the body via `ValueSpecificationBuilder`. When
`srcType` is empty (fallback for a malformed function) the parameter falls back
to `Any[1]` — the validator surfaces the underlying error separately.

### 5.4 Third pass — `ClassMappingThirdPassBuilder`

Resolves explicit `~primaryKey` column names against the function's typed
`RelationType`. If `~primaryKey` is empty, leaves the set's `primaryKey` empty
and defers to runtime auto-inference (§8).

```java
public SetImplementation visit(RelationFunctionClassMapping classMapping)
{
    ...
    if (classMapping.primaryKey == null || classMapping.primaryKey.isEmpty())
    {
        return setImpl;   // auto-inferred at SQL-gen time
    }

    RichIterable<? extends Column<?, ?>> relationColumns = getRelationFunctionColumns(setImpl);
    if (relationColumns.isEmpty())
    {
        throw new EngineException(
            "Cannot resolve primary key columns: relation function '"
            + setImpl._relationFunction()._functionName()
            + "' does not return a concrete RelationType. "
            + "Ensure the function body produces a typed relation (e.g., #>{db.table}#).", ...);
    }

    MutableList<Column<?, ?>> resolvedPK = Lists.mutable.empty();
    for (String pkName : classMapping.primaryKey)
    {
        Column<?, ?> col = (Column<?, ?>) relationColumns.detect(c -> pkName.equals(c._name()));
        if (col == null)
        {
            String available = relationColumns.collect(Column::_name).makeString(", ");
            throw new EngineException(
                "Primary key column '" + pkName + "' declared in class mapping '" + id
                + "' (mapping '" + mappingPath + "') is not part of the columns returned by the relation function."
                + " Available columns: [" + available + "]." , ...);
        }
        resolvedPK.add(col);
    }
    setImpl._primaryKey(resolvedPK);
    return setImpl;
}

// Reads columns off the last-expression RelationType, or [] for Relation<Any>.
private static RichIterable<? extends Column<?, ?>> getRelationFunctionColumns(
    RelationFunctionInstanceSetImplementation setImpl)
{ ... }
```

### 5.5 Property mapping: `PropertyMappingBuilder.visit(RelationFunctionPropertyMapping)`

Builds the M3 `RelationFunctionPropertyMapping` **skeleton only** — no
multiplicity/type checks here. The `_valueFn` slot is filled by SecondPass, and
validation runs afterwards in `MappingValidator.validateRelationFunctionClassMapping`.

Key attachments:

- **Property resolution** via `resolveRelationFunctionMappedProperty`:
  - Local property (`localMappingProperty` present) → normal
    `HelperMappingBuilder.getMappedProperty`.
  - Otherwise resolves against the immediate parent set's class (handles the
    embedded case where the parser's `_class` pointer was rewritten by
    §5.6).
- **Local property fields** (`_localMappingProperty`,
  `_localMappingPropertyType`, `_localMappingPropertyMultiplicity`).
- **Binding transformer** — attaches `BindingTransformer` after verifying the
  property is a `Class` and its type is in the binding's `modelUnit`.
- **Enumeration transformer** — attaches the named `EnumerationMapping` from
  the parent mapping's registered enum mappings.

### 5.6 Property mapping: `PropertyMappingBuilder.visit(RelationFunctionEmbeddedPropertyMapping)`

Builds an `EmbeddedRelationFunctionSetImplementation`. Two shapes:

```java
boolean isInline = propertyMapping.propertyMappings == null
                || propertyMapping.propertyMappings.isEmpty();
String inlineTargetId = propertyMapping.setImplementationId != null
                        && !propertyMapping.setImplementationId.isEmpty()
                            ? propertyMapping.setImplementationId
                            : propertyMapping.id;

String selfId, targetId;
if (isInline && inlineTargetId != null)
{
    selfId   = sourceId + "_" + propertyMapping.property.property;
    targetId = inlineTargetId;         // points to the separately-declared set
}
else
{
    String embeddedId = inlineTargetId != null
                          ? inlineTargetId
                          : sourceId + "_" + propertyMapping.property.property;
    selfId   = embeddedId;
    targetId = embeddedId;             // self-referential (normal embedded)
}
```

**Sub-property class rewrite.** The parser walker stamps every child sub-
property's `_class` with the OUTER class mapping's class (it has no type
information at parse time). The embedded builder rewrites each child pointer
to the embedded target class so `HelperMappingBuilder.getMappedProperty`
resolves the property against the right class — without this, `address ( city:
CITY )` would look up `city` on `Person` instead of `Address`.

```java
String targetClassPath = HelperModelBuilder.getElementFullPath(
    (PackageableElement) property._genericType()._rawType(),
    context.pureModel.getExecutionSupport());
propertyMapping.propertyMappings.forEach(subPm ->
{
    if (subPm.property != null && subPm.localMappingProperty == null)
    {
        subPm.property._class = targetClassPath;
    }
});
```

The inner `propertyMappings` are then compiled recursively with a fresh
`PropertyMappingBuilder` whose `immediateParent` is the new embedded set.

### 5.7 Bare-column fast-path helper — `RelationFunctionPropertyMappingTools`

`asColumnRef(pm)` returns `Optional<String>` when the M3 `_valueFn` body is
exactly a single `$src.<col>` accessor — i.e. was authored as the bare-column
form (or the equivalent explicit `$src.<col>` expression). Consumers that need
the column name (SQL push-down fast paths, IDE display, debug printers)
encapsulate the pattern match here rather than re-implementing it:

```java
public static Optional<String> asColumnRef(RelationFunctionPropertyMapping pm)
{
    if (pm == null || pm._valueFn() == null) return Optional.empty();
    ...
    // matches SimpleFunctionExpression whose parametersValues is [VariableExpression("src")]
    // and whose propertyName instance value carries the accessed column name.
}
```

Deliberately conservative — a complex expression that happens to evaluate to a
single column at runtime is NOT matched.

### 5.8 Validation — `MappingValidator`

Two arms run inside `validateRelationFunctionClassMapping`:

**Protocol-side (inline-embedded id resolution):** for every
`RelationFunctionEmbeddedPropertyMapping` with `id` set and empty
`propertyMappings`, the `id` must match a `RelationFunctionClassMapping` in the
same protocol Mapping:

```java
if (pm.id != null && !pm.id.isEmpty()
    && (pm.propertyMappings == null || pm.propertyMappings.isEmpty())
    && !classMappingIds.contains(pm.id))
{
    throw new EngineException(
        "The set implementation '" + pm.id + "' referenced in the inline embedded mapping "
        + "for property '" + pm.property.property + "' does not exist in the mapping " + mappingPath, ...);
}
```

**Pure-graph side:** for every `RelationFunctionInstanceSetImplementation`:

- function has zero parameters;
- return type is `Relation<...>`;
- for each `RelationFunctionPropertyMapping._valueFn` (recursively into
  embedded sets):
  - the body's inferred multiplicity is subsumed by the property multiplicity;
  - the body's inferred raw type is a subtype of the property raw type — this
    check is skipped when a transformer (`BindingTransformer` or
    `EnumerationMapping`) is present, since the transformer is responsible for
    the conversion.

Errors mirror legend-pure's `RelationFunctionInstanceSetImplementationValidator`
verbatim (e.g. `"Multiplicity Error: The property '...' has a multiplicity range
of [1] when the given expression has a multiplicity range of [0..1]"`).

---

## 6. Runtime helpers (Pure)

There is no separate "transform" phase — property mappings are stored as
`RelationFunctionPropertyMapping` / `EmbeddedRelationFunctionSetImplementation`
throughout the Pure graph. Runtime SQL generation and property resolution use
the helpers below.

### 6.1 `findPropertyMapping` — `EmbeddedRelationFunctionSetImplementation` arm

Located in `core_relational/relational/helperFunctions/helperFunctions.pure`.
When the current property mapping is an `EmbeddedRelationFunctionSetImplementation`,
resolution tries in order:

1. **Direct lookup** in the embedded set's own `propertyMappings`.
2. **Inline-target lookup**: if `targetSetImplementationId != id`, resolve via
   `mapping->_classMappingByIdRecursive`.
3. **Router-supplied mappings** fallback.

```
| if($currentPropertyMapping->at(0)->instanceOf(EmbeddedRelationFunctionSetImplementation),
| let originalEmbedded = $currentPropertyMapping->cast(@EmbeddedSetImplementation);
  let directEmbeddedPM = $originalEmbedded->map(c | $c->_propertyMappingsByPropertyName($propertyName));
  let inlineTargetIds  = $originalEmbedded.targetSetImplementationId
                            ->filter(t | !$t->isEmpty() && $originalEmbedded.id != $t);
  let result = if([
    pair(|!$directEmbeddedPM->isEmpty(), | $directEmbeddedPM),
    pair(|!$inlineTargetIds->isEmpty(), | $mapping->_classMappingByIdRecursive($inlineTargetIds)
                                                   ->filter(c | !$c->instanceOf(EmbeddedSetImplementation))
                                                   ->cast(@InstanceSetImplementation)
                                                   ->map(c | $c->_propertyMappingsByPropertyName($propertyName))),
    pair(|!$propertyMappingFromRouter->isEmpty(), | $propertyMappingFromRouter)
  ], | []);
  ...
```

### 6.2 `inlineEmbeddedRelationFunctionMapping` — mappingExtension.pure

Called from `findMappingsFromProperty` when navigating through an inline embedded
set. Resolves the inline target and copies its property mappings onto the
embedded set (re-owning them to the embedded owner):

```
function meta::pure::router::routing::inlineEmbeddedRelationFunctionMapping(
    e: EmbeddedRelationFunctionSetImplementation[1], m: Mapping[1]
): EmbeddedRelationFunctionSetImplementation[1]
{
    let inlineTargetId = $e.targetSetImplementationId;
    let cm = $m->_classMappingByIdRecursive($inlineTargetId)
                ->filter(c | !$c->instanceOf(EmbeddedSetImplementation));
    assertEquals(1, $cm->size(), | ...);
    let pmaps = $cm->cast(@InstanceSetImplementation)->toOne()->allPropertyMappings()
                    ->map(pm | ^$pm(owner = $e.owner, sourceSetImplementationId = $e.sourceSetImplementationId));
    ^$e(id = $inlineTargetId, propertyMappings = $pmaps);
}
```

### 6.3 Primary-key helpers (relational side)

Located in `core_relational/relational/helperFunctions/helperFunctions.pure`.

```
// TableAliasColumn instances anchored on a RelationFunction alias for the
// set's primaryKey columns.
function meta::relational::mapping::getRelationFunctionPkAsTableAliasColumns(
    classMapping: RelationFunctionInstanceSetImplementation[1]): TableAliasColumn[*]
{
    let alias = ^TableAlias(name = $classMapping.id,
                             relationalElement = ^RelationFunction(owner = $classMapping));
    $classMapping.primaryKey->map(c |
        let colType = $c.classifierGenericType.typeArguments->at(1).rawType->toOne();
        ^TableAliasColumn(
            alias  = $alias,
            column = ^RelationFunctionColumn(
                column = $c,
                name   = $c.name->toOne()->addQuotesIfNecessary(),
                type   = meta::relational::mapping::pureTypeToDataType($colType)->toOne()
            )
        )
    );
}

// Lazy accessor used at SQL-gen time to auto-infer PK columns when the
// compile-time set had no explicit ~primaryKey.
function meta::relational::mapping::ensureRelationFunctionPrimaryKeyResolved(
    rf: RelationFunctionInstanceSetImplementation[1],
    extensions: Extension[*]): RelationFunctionInstanceSetImplementation[1]
{
    if($rf.primaryKey->isEmpty(),
        | ^$rf(primaryKey = $rf->resolveRelationFunctionPrimaryKey([], $extensions)),
        | $rf);
}
```

The relational store also registers a synthetic `RelationElementAccessorExtension`
(see §8) whose resolver reads PK columns from `Table` / `View` via
`tablePrimaryKeyLeaf`.

---

## 7. Routing (`cluster.pure`, `routing.pure`)

### 7.1 `storeContractForSetImplementation`

Determines which `StoreContract` and `Store` handle a given set implementation:

| Set type | Resolution |
|----------|------------|
| `RelationFunctionInstanceSetImplementation` | Reads the store from the routed relation function's `StoreClusteredValueSpecification` (asserts the function has been routed) |
| `InstanceSetImplementation` | Store contract from extension + `resolveStoreFromSetImplementation` |
| `OperationSetImplementation` (union) | Resolves each leaf recursively; deduplicated store must be unique (`->toOne()` enforces single-store) |
| `EmbeddedSetImplementation` | Delegates to `owner` |

```
storeContractForSetImplementation(setImpl, mapping, extensions)
    $setImpl->match([
      t: RelationFunctionInstanceSetImplementation[1]|
         let vs = $t.relationFunction.expressionSequence->evaluateAndDeactivate();
         assert($vs->size() == 1, 'Function used in Relation function class mapping can only have a single expression!');
         assert($vs->at(0)->instanceOf(StoreClusteredValueSpecification), ...);
         let store = $vs->at(0)->cast(@StoreClusteredValueSpecification).store;
         pair(meta::pure::extension::storeContractFromStore($extensions, $store), $store);,
      ...
      o: OperationSetImplementation[1]| ... all leaves must resolve to one store ...,
      e: EmbeddedSetImplementation[1]| $e.owner->toOne()->cast(@SetImplementation)
                                          ->storeContractForSetImplementation($mapping, $extensions);
    ])
```

### 7.2 `potentiallyRouteRelationFunctionSet` (single-set routing)

Pushes the relation function through the router so its expression sequence is a
`ClusteredValueSpecification`:

```
potentiallyRouteRelationFunctionSet(s, mapping, runtime, extensions)
    $s->match([
      t: RelationFunctionInstanceSetImplementation[1]|
          ^$t(relationFunction = $t->potentiallyRouteRelationFunction($mapping, $runtime, $extensions)),
      e: EmbeddedSetImplementation[1]| $e,
      s: InstanceSetImplementation[1]| $s,
      o: OperationSetImplementation[1]|
          ^$o(parameters = $o.parameters->map(i | ^$i(setImplementation = ...
                              ->potentiallyRouteRelationFunctionSet($mapping, $runtime, $extensions))))
    ])
```

`isRelationFunctionRouted(rf)` returns true when the function's first
expression is a `ClusteredValueSpecification`. `potentiallyRouteRelationFunction`
either returns the already-routed function or calls `routeFunction(...)` to
produce one.

### 7.3 `potentiallyRouteRelationFunctionSets` (class-level routing)

Called during `processClass` and `processProperty` routing. Partitions the
class's set implementations into RF and non-RF; routes the un-routed RF ones
(consulting the `classMappingsByClass` cache); recurses through any
`OperationSetImplementation` or `EmbeddedRelationFunctionSetImplementation`
leaves so union branches and embedded sets are wired end-to-end. Updates the
`StoreMappingRoutingStrategy.classMappingsByClass` cache with the routed sets
so subsequent property navigations short-circuit.

---

## 8. Primary-Key Inference

When `~primaryKey` is omitted the runtime derives PK columns from the relation
function's body. The algorithm lives in
`legend-engine-pure-code-compiled-core/.../core/pure/mapping/relationFunctionMapping.pure`
and is driven by store-specific extensions.

### 8.1 `RelationElementAccessorExtension`

A `ModuleExtension` subtype that stores register to teach the PK inferencer
about their leaf accessors:

```
Class meta::pure::mapping::relation::RelationElementAccessorExtension extends ModuleExtension
[
   $this.module == meta::pure::mapping::relation::relationElementAccessorModuleExtensionName()
]
{
   instancePrimaryKeyResolver : Function<{InstanceValue[1], Extension[*]->String[*]}>[1];
}
```

The relational store registers one in `helperFunctions.pure`'s
`syntheticRelationalAccessorExtension`:

```
instancePrimaryKeyResolver = {iv: InstanceValue[1], ext: Extension[*] |
    $iv.values->map(v | $v->match([
        rsa: meta::pure::store::RelationStoreAccessor<Any>[1]
            → $rsa.sourceElement->meta::relational::mapping::tablePrimaryKeyLeaf(),
        a: Any[*] → []
    ]))->cast(@String)
}
```

`tablePrimaryKeyLeaf` reads PK column names from `Table.primaryKey` or
`View.primaryKey`.

### 8.2 Top-level entry points

```
resolveRelationFunctionPrimaryKey(r, explicitColumnNames, extensions): Column<Nil,Any|*>[*]
    let pkNames = if($explicitColumnNames->isEmpty(),
        | $r->resolveRelationFunctionPrimaryKeyColumnNames($extensions),
        | $explicitColumnNames);
    let cols = $r->extractRelationColumns();
    $pkNames->map(name | $cols->filter(c | $c.name == $name)->first());

extractRelationColumns(r): Column<Nil,Any|*>[*]
    // Reads the Column list off the last-expression RelationType, or [] if Relation<Any>.
```

### 8.3 Recursive body walker — `inferPrimaryKeyColumnNames`

```
inferPrimaryKeyColumnNames(vs, extensions)
    $vs->match([
        iv:  InstanceValue[1] |
            // Delegate to every registered RelationElementAccessorExtension,
            // flatten results (non-handling resolvers return []; composite PKs survive).
            $extensions.moduleExtensions
                ->filter(m | $m->instanceOf(RelationElementAccessorExtension))
                ->cast(@RelationElementAccessorExtension)
                ->map(ext | $ext.instancePrimaryKeyResolver->eval($iv, $extensions))
                ->cast(@String),
        cvs: ClusteredValueSpecification[1] |
            $cvs.val->inferPrimaryKeyColumnNames($extensions),
        fe:  SimpleFunctionExpression[1] |
            $fe->inferPrimaryKeyColumnNamesFromFunctionExpression($extensions),
        a:   Any[*] | []
    ])
```

### 8.4 Platform relation operators — `inferPrimaryKeyColumnNamesFromFunctionExpression`

User-defined helpers are inlined (recurse into the body). Platform relation
operators are handled by name; behaviour:

| Operator | Result |
|----------|--------|
| `filter`, `limit`, `drop`, `slice`, `sort`, `extend(*)`, `select` (no arg), `distinct` (no arg) | leftPK |
| `select(colSpec)`, `select(colSpecArray)` | leftPK ∩ projected cols (`applySelectToPK`) |
| `rename(oldSpec, newSpec)` | leftPK with old→new name substitution (`applyRenameToPK`) |
| `distinct(colSpecArray)` | the distinct-by cols |
| `groupBy(cols, aggs...)` | group cols |
| `aggregate(aggs...)` | `[]` |
| `join(l, r, INNER | LEFT, cond)` | leftPK ∪ rightPK — `JoinKind` extracted from either `InstanceValue` or `extractEnumValue` SFE |
| `join(l, r, RIGHT | FULL | unknown, cond)` | `[]` |
| `asOfJoin(l, r, ...)` | leftPK ∪ rightPK |
| anything else | `[]` |

---

## 9. SQL Generation

### 9.1 `processRelationFunctionClassMapping`

The central entry point for `getAll` on a Relation-backed class:

```
processRelationFunctionClassMapping(r, vars, state, milestoningContext, joinType, nodeId, context, extensions)
    // 1. Auto-infer PK if not set at compile time
    let resolved = if($r.primaryKey->isEmpty(),
        | let pkCols = $r->resolveRelationFunctionPrimaryKey([], $extensions);
          ^$r(primaryKey = $pkCols),
        | $r);

    // 2. Route the relation function so its expressionSequence carries a
    //    StoreClusteredValueSpecification / ClusteredValueSpecification.
    let routedRelationFunction = $resolved
        ->potentiallyRouteRelationFunctionSet($state.mapping->toOne(), $extensions)
        ->cast(@RelationFunctionInstanceSetImplementation).relationFunction;

    // 3. Evaluate the (routed) body to a SelectSQLQuery-carrying cursor.
    let relationExpression = $routedRelationFunction.expressionSequence
        ->evaluateAndDeactivate()->at(0)->cast(@ClusteredValueSpecification).val;
    let newCursor = ^SelectWithCursor(select = ^SelectSQLQuery(), milestoningContext = $milestoningContext);
    let newState  = defaultState($state.mapping, $state.inScopeVars, $state.idToClassMapping);
    let cursor    = $relationExpression->processValueSpecification(
                        [], $newCursor, $vars, $newState,
                        $joinType, $nodeId, ^List<ColumnGroup>(), $context, $extensions
                    )->toOne()->cast(@SelectWithCursor);

    // 4. Wrap in a sub-select so subsequent filter/project/sort operate on the
    //    materialised relation output.
    let newSelect = $cursor.select->moveSelectQueryToSubSelect(
                       $cursor.currentTreeNode, [], $nodeId, $context, $extensions);
    ^$cursor(select = $newSelect, currentTreeNode = $newSelect.data);
```

### 9.2 `processGetAll` dispatch

```
processGetAll(expression, setImplementation, parameters, vars, state, joinType, nodeId, context, extensions)
    let processSetImpl = {r: InstanceSetImplementation[1] |
        $r->match([
            rr: RootRelationalInstanceSetImplementation[1]  → processGetAll($rr, $rr.class, ...),
            rf: RelationFunctionInstanceSetImplementation[1] →
                let mc = getMilestoningContextForAll(...);
                processRelationFunctionClassMapping($rf, $vars, $state, $mc, ...)
        ])
    };

    $setImplementation->match([
        r:  RootRelationalInstanceSetImplementation[1] | $processSetImpl->eval($r),
        r:  RelationFunctionInstanceSetImplementation[1]  | processRelationFunctionClassMapping($r, ...),
        r:  CrossSetImplementation[1]                     | ... placeholder ...,
        o:  OperationSetImplementation[1] |
            let setImpls = $o->resolveOperation($state.mapping->toOne())->cast(@InstanceSetImplementation);
            if($setImpls->size() == 1,
                | $processSetImpl->eval($setImpls->at(0)),
                | buildUnion($setImpls, ..., $milestoningContext, ..., $context, $extensions)
                  ... build unionBase alias + expanded columns ...
            )
    ])
```

### 9.3 RFPM → downstream PM synthesis (`transformRelationFunctionPropertyMappingToRelational`)

Property navigation eventually reaches a `RelationFunctionPropertyMapping`. It
is transformed to a downstream `RelationalPropertyMapping` or semi-structured
variant just-in-time by `transformRelationFunctionPropertyMappingToRelational`.

#### Step 1 — synthetic RF cursor with placeholder TACs

```
buildSyntheticRfCursor(cm)
    let allCols = $cm->extractRelationColumns();
    let rfColumns = $allCols->map(c |
        let colType = $c.classifierGenericType.typeArguments->at(1).rawType->toOne();
        ^RelationFunctionColumn(
            column = $c,
            name   = $c.name->toOne()->addQuotesIfNecessary(),
            type   = pureTypeToDataType($colType)->toOne()
        )
    );
    let rfRelation = ^RelationFunction(owner = $cm, columns = $rfColumns);
    let rfAlias    = ^TableAlias(name = $cm.id, relationalElement = $rfRelation);
    let rfNode     = ^RootJoinTreeNode(alias = $rfAlias);
    ^SelectWithCursor(select = ^SelectSQLQuery(data = $rfNode), currentTreeNode = $rfNode)
```

#### Step 2 — bind `$src`, evaluate, detect variant

```
evaluateRfpmValueFn(pm, oldSrcOperation, state, ...)
    let cm  = $state->getClassMappingById($pm.sourceSetImplementationId)
                     ->toOne()->cast(@RelationFunctionInstanceSetImplementation);
    let syntheticCursor = buildSyntheticRfCursor($cm);

    // The lambda's formal parameter name (conventionally `src`, but user-renamable)
    // is read off the lambda's FunctionType via updateFunctionParamScope.
    let lambdaFnType = $pm.valueFn.classifierGenericType.typeArguments.rawType->toOne()->cast(@FunctionType);
    let evalState    = ^$state(inFilter = false);
    let stateWithSrc = $evalState->updateFunctionParamScope($lambdaFnType, $syntheticCursor);

    let rhsBody   = $pm.valueFn.expressionSequence->evaluateAndDeactivate()->last()->toOne();
    let resultSwc = $rhsBody->processValueSpecification(
                       $pm, $syntheticCursor, newMap([]), $stateWithSrc,
                       $joinType, $nodeId, $aggFromMap, $context, $extensions
                    )->cast(@SelectWithCursor);

    ^RfpmValueFnResult(
        relop     = $resultSwc.select.columns->toOne(),
        isVariant = $rhsBody->expressionTouchesVariant($stateWithSrc)
    )
```

The resulting `relop` is a tree whose leaves are placeholder `TableAliasColumn`s
carrying `RelationFunctionColumn`s with an empty `column.owner`. Those
placeholders get resolved against the real source at column-navigation time (see
§9.4).

#### Step 3 — dispatch by binding / variant / target type

```
transformRelationFunctionPropertyMappingToRelational(pm, oldSrcOperation, state, ...)
    let cm        = $state->getClassMappingById($pm.sourceSetImplementationId)->toOne()
                              ->cast(@RelationFunctionInstanceSetImplementation);
    let isBinding = $pm.transformer->isNotEmpty() && $pm.transformer->toOne()->instanceOf(BindingTransformer);
    let rhs       = evaluateRfpmValueFn($pm, $oldSrcOperation, $state, ...);

    if($isBinding,
        | // Mode 1 — Binding: target is always the property's return type (a Class).
          let targetClass = $pm.property->functionReturnType().rawType->toOne()->cast(@Class<Any>);
          buildSsEmbeddedSetForRfpm($pm, $cm, $targetClass, $rhs.relop);,
        | // Mode 2 — Lift: dispatch by variant-ness / target type.
          buildRelationalPropertyMappingForRfpm($pm, $cm, $rhs)
    )

// Mode-2 dispatcher (variant-ness × target-type)
buildRelationalPropertyMappingForRfpm(pm, cm, rhs)
    assertRfpmTargetTypeSupported($pm);
    let propType = $pm.property.genericType.rawType->toOne();
    if(!$rhs.isVariant,
        | buildRelationalPropertyMappingForRfpm($pm, $rhs.relop),                   // plain RPM
        | if($propType->instanceOf(Class),
            | buildSsEmbeddedSetForRfpm($pm, $cm, $propType->cast(@Class<Any>), $rhs.relop),   // SS-embedded
            | buildSsRelationalPropertyMappingForRfpm($pm, $rhs.relop)                          // SS-RPM
        )
    )
```

Downstream flavours:

- **`RelationalPropertyMapping`** — for non-variant valueFn bodies. Standard
  relational chain, transformer flows through.
- **`SemiStructuredEmbeddedRelationalInstanceSetImplementation`** — for
  Binding-bearing RFPMs *and* for variant valueFn with a Class target.
  Constructed via `buildSsEmbeddedSetForRfpm`, backed by a dummy
  `RootRelationalInstanceSetImplementation` (`buildRfpmDummyRootSet`) — needed
  because downstream code (`setMappingOwner` lookups, PK resolution) expects the
  parent slot to be Root-typed.
- **`SemiStructuredRelationalPropertyMapping`** — for variant valueFn whose
  target is a primitive / Enumeration / `Variant`.
- **Rejected**: structural containers `Map`, `List`, `Pair` are Classes but
  unsupported (`assertRfpmTargetTypeSupported` hard-fails).

### 9.4 Placeholder-TAC resolution

`resolveTableAliasColumn` recognises placeholder `RelationFunctionColumn`s by
checking `column.owner->isEmpty()`. When the current tree node's relational
element is a `SelectSQLQuery`, it either reuses an existing matching column or
adds the placeholder as a projected column of the outer select and returns a
new `TableAliasColumn` bound to the outer alias.

```
let isPlaceholderMatch = $column->instanceOf(RelationFunctionColumn) && $column.owner->isEmpty();
if($foundColumn->isNotEmpty() && ($isPlaceholderMatch || $column.owner == $foundColumnOwner),
    | // reuse the found column
      pair($srcOperation, ^$column(name = $foundColumn->toOne()->extractColumnName())),
    | // add to nested select
      $t->addMissingColumnToNestedSelect($s, $column, ...)
)
```

### 9.5 Variant detection — `isVariantInput` / `expressionTouchesVariant`

Located in `pureToSQLQuery_variant.pure`. In addition to matching against the
value spec's generic type, `isVariantInputImpl` recognises RFPM column accessors
that resolve to `SemiStructured` / `Object` / `Array` on a placeholder
`RelationFunction`, flagging the expression as variant-touching without
requiring the property to be typed as `Variant`.

### 9.6 PK auto-inference at execution boundary

Certain call sites re-check PK resolution just before it is needed, using
`ensureRelationFunctionPrimaryKeyResolved`:

```
if($f.parametersValues->at(0)->cast(@StoreMappingRoutedValueSpecification).sets->toOne()
       ->instanceOf(RelationFunctionInstanceSetImplementation),
    | let rs = $f.parametersValues->at(0)->cast(@StoreMappingRoutedValueSpecification).sets->toOne()
                  ->cast(@RelationFunctionInstanceSetImplementation)
                  ->meta::relational::mapping::ensureRelationFunctionPrimaryKeyResolved($extensions);
      ...
);
```

---

## 10. Union SQL generation (`pureToSQLQuery_union.pure`)

`buildUnion` accepts `InstanceSetImplementation[*]` and dispatches per-leaf:

```
let simpleAllQueries = $setImpls->map(r | $r->match([
    rr: RootRelationalInstanceSetImplementation[1] |
        processGetAll($rr, $rr.class, $joinType, $nodeId, ...),
    rf: RelationFunctionInstanceSetImplementation[1] |
        let swc = processRelationFunctionClassMapping($rf, newMap([]), ^$state(importDataFlowAddFks=false),
                                                       $milestoningContext, $joinType, $nodeId, ...);
        ^$swc(currentTreeNode = $swc.select.data)
]));
```

**Milestoning columns.** RF leaves return an empty column list (no physical
table to inspect):

```
let milestoningColumns = $setImpls->map(s | $s->match([
    rr: RootRelationalInstanceSetImplementation[1] |
        $rr.mainTableAlias.relationalElement->findMainNamedRelation()
            ->match([t: Table[1] | $t.milestoning->getAllTemporalColumns(),
                     r: NamedRelation[1] | ^List<Column>(values=[])]),
    rf: RelationFunctionInstanceSetImplementation[1] | ^List<Column>(values=[])
]));
```

**Column expansion for non-merge-compatible unions.** When joins cannot be
merged, per-leaf columns are synthesised from the leaf's already-materialised
`SelectSQLQuery.columns`:

```
let allColumns = $setImpl->match([
    rr: RootRelationalInstanceSetImplementation[1] | $rr->mainRelation().columns->cast(@Column),
    rf: RelationFunctionInstanceSetImplementation[1] |
        $q.data->toOne().alias.relationalElement->match([
            s: SelectSQLQuery[1] |
                $s.columns->cast(@Alias)->map(a | ^Column(name = $a.name,
                                                          type = ^meta::relational::metamodel::datatype::Integer())),
            o: RelationalOperationElement[1] | []->cast(@Column)
        ])
]);
```

**Target-side normalisation.** When building the union target, the state-
resolved set is normalised down to `RootRelationalInstanceSetImplementation` or
`RelationFunctionInstanceSetImplementation`:

```
let normalizedSet = $set->toOne()->match([
    r:   RootRelationalInstanceSetImplementation[1]     | $r,
    e:   EmbeddedRelationalInstanceSetImplementation[1] | $e.setMappingOwner,
    erf: EmbeddedRelationFunctionSetImplementation[1]   | $erf.owner,
    rf:  RelationFunctionInstanceSetImplementation[1]   | $rf,
    a:   OperationSetImplementation[*]                  | fail(...),
    s:   SetImplementation[1]                           | fail(...)
])->cast(@InstanceSetImplementation)->toOne();
assert($normalizedSet->instanceOf(RootRelationalInstanceSetImplementation)
       || $normalizedSet->instanceOf(RelationFunctionInstanceSetImplementation), ...);
```

For a single-leaf target the union simply invokes
`processRelationFunctionClassMapping` (or the relational equivalent); >1
leaves recurse into `buildUnion`.

**FK discovery.** `findFkListForEachSet` walks each set's `allPropertyMappings`
and pulls PK-related `TableAliasColumn`s. For RF sets, aliases are filtered by
`RelationFunction.owner == rf`. For embedded sets that own a RF parent, the
walker descends through `EmbeddedRelationFunctionSetImplementation → owner`.

**Same-relation equality.** `isSameRelation` treats two `RelationFunction`s as
identical when their `owner` sets are the same.

**Unique-name generation.** `buildUniqueName` has an arm for `RelationFunction`:

```
rf: RelationFunction[1] | 'rf(' + $rf.owner.id + ')'
```

**Union property mapping resolution** (`findUnionPropertyMapping`) — the state-
based lookup handles RF sets and RF embedded sets:

```
$state->getClassMappingById($r.sourceSetImplementationId)->match([
    r:   RootRelationalInstanceSetImplementation[1]   | $r,
    e:   EmbeddedRelationalInstanceSetImplementation[1] | $e.setMappingOwner,
    erf: EmbeddedRelationFunctionSetImplementation[1] | $erf.owner->toOne()->cast(@RelationFunctionInstanceSetImplementation),
    rf:  RelationFunctionInstanceSetImplementation[1] | $rf
])
```

---

## 11. Composer (`DEPRECATED_PureGrammarComposerCore`)

Round-trip serialisation writes back both source and RHS forms.

**Class-mapping source:**

```java
if (classMapping.relationFunction != null)
{
    sourceLine = getTabString(...) + "~func " + classMapping.relationFunction.path + "\n";
}
else if (classMapping.sourceLambda != null)
{
    // sourceLambda wraps the inline expression in a zero-arg lambda, so we
    // render the body directly (stripping the synthetic `|` prefix).
    sourceLine = getTabString(...) + "~src " + renderRelationLambdaBody(classMapping.sourceLambda) + "\n";
}
```

**Property RHS:**

```java
String rhs = propertyMapping.valueFn != null
                ? renderRelationLambdaBody(propertyMapping.valueFn)
                : PureGrammarComposerUtility.convertIdentifier(propertyMapping.column, ...);
return renderPossibleLocalMappingProperty(propertyMapping)
     + (propertyMapping.bindingTransformer != null ? ": Binding " + ... + " " : "")
     + (propertyMapping.enumMappingId      != null ? ": EnumerationMapping " + ... + " " : "")
     + ": " + rhs;
```

**Embedded property:**

```java
if (propertyMapping.id != null && propertyMapping.propertyMappings.isEmpty())
    return propertyMapping.property.property + " () Inline [" + propertyMapping.id + "]";
else
    return propertyMapping.property.property + "\n(...)"
```

`renderRelationLambdaBody` renders a single-expression lambda body directly (no
`|`); falls back to the full lambda visitor for multi-expression bodies.

---

## 12. Protocol transfer (`vX_X_X/transfers/mapping.pure`)

The engine-side JSON protocol carries `~func` and `~src` as two mutually
exclusive nullable fields, and `column`/`valueFn` similarly. Transfer between
the compiled Pure metamodel (which only has `_relationFunction` typed
`FunctionDefinition` and `_valueFn` typed `LambdaFunction`) and the protocol:

**Class mapping — Pure → protocol:**

```
transformRelationFunctionInstanceSetImplementation(r, mapping, extensions)
    ^RelationFunctionClassMapping(
        ...,
        relationFunction = $r.relationFunction->match([
            c: ConcreteFunctionDefinition<Any>[1] |
                ^PackageableElementPointer(type = FUNCTION, path = $c->elementToPath()),
            any: FunctionDefinition<Any>[1] | []
        ]),
        sourceLambda = $r.relationFunction->match([
            c: ConcreteFunctionDefinition<Any>[1] | [],
            l: LambdaFunction<Any>[1]             | $l->transformLambda($extensions)
        ]),
        propertyMappings = $r.propertyMappings->map(pm | $pm->transformRelationFunctionPropertyMapping(...)),
        primaryKey = $r.primaryKey.name
    )
```

**Property mapping — Pure → protocol:** always emits `valueFn` (never `column`).
SecondPass has already lowered bare-column authoring to `{$src.<col>}`, so the
round-trip is lossless and avoids brittle pattern-matching to recover the
sugar form:

```
r: meta::pure::mapping::relation::RelationFunctionPropertyMapping[1] |
    ^RelationFunctionPropertyMapping(
        _type    = 'relationFunctionPropertyMapping',
        property = ...,
        valueFn  = $r.valueFn->transformLambda($extensions),
        source   = $r.sourceSetImplementationId,
        target   = $r.targetSetImplementationId,
        enumMappingId = $r.transformer->cast(@EnumerationMapping<Any>).name,
        localMappingProperty = ...
    )
```

**Embedded property mapping — Pure → protocol:** picks
`InlineEmbeddedRelationFunctionPropertyMapping` (empty `propertyMappings`) or
`EmbeddedRelationFunctionPropertyMapping` (populated).

---

## 13. Decision Cheat-sheet

| Question | Answer |
|----------|--------|
| Difference between `~func` and `~src`? | `~func` references an existing Pure function by descriptor; `~src` inlines a zero-arg expression that evaluates to `Relation<Any>`. The compiler treats both uniformly after wrapping `~src` in a synthetic lambda. |
| What property RHS forms are supported? | Bare column identifier (lowered to `{$src.<col>}`) or a full Pure expression over `$src`. |
| When should I omit `~primaryKey`? | When the function body's leaf accessor is a table/view your store's `RelationElementAccessorExtension` knows about, and the operator chain preserves PK (see §8). Add an explicit `~primaryKey` when the body is opaque or transforms columns the operator table doesn't cover. |
| Can I map multiple PK columns? | Yes: `~primaryKey: [COL1, COL2]` |
| Property types supported? | Primitives, Enumerations (with `EnumerationMapping`), `Variant`, and complex `Class` types (with `Binding` for binding-style, or a variant-touching valueFn for lift-style). Collection multiplicities (`[*]`) on the property are supported when the valueFn body's multiplicity is subsumed by the property's declared multiplicity. |
| What property types are explicitly rejected? | Structural containers `Map`, `List`, `Pair` (they are Classes but unsupported by RFPM lift). |
| Where does multiplicity/type validation happen? | `MappingValidator.validateRelationFunctionClassMapping` — after SecondPass has built `_valueFn` and the type inferencer has run. Skipped for the transformer case. |
| How is an enum property compiled? | The property's `_valueFn` is built with body `{$src.<col>}`; the `EnumerationMapping` is attached as `_transformer`. Enum push-down runs during property processing. |
| Normal vs inline embedded — when to use which? | Normal: all sub-object columns come from the same relation function. Inline: sub-object has its own independently-declared class mapping (possibly a different function). |
| Can inline embedded use a different relation function? | Yes. The inline target set is fully independent and can declare its own `~func` / `~src`. |
| How does `$x.address.city` resolve at execution time? | Via `findPropertyMapping`'s `EmbeddedRelationFunctionSetImplementation` arm — direct child lookup for normal embedded; `_classMappingByIdRecursive` for inline embedded. Router-time normalisation of inline embedded happens in `inlineEmbeddedRelationFunctionMapping`. |
| Can a `Relation` mapping participate in a `union`? | Yes. All leaves must resolve to the same store. `buildUnion` dispatches per-leaf to `processRelationFunctionClassMapping` or `processGetAll`. |
| Can I mix Relation and Relational leaves in a union? | Yes, as long as they share the same store. |
| What happens to milestoning columns in a union with an RF leaf? | The RF leaf contributes an empty milestoning column list; temporal filtering is not applied to that branch. |
| Cross-store union? | Not supported. `->toOne()` on store deduplication in `storeContractForSetImplementation` enforces single-store. |
| How does semi-structured / variant lift work? | `transformRelationFunctionPropertyMappingToRelational` evaluates the property's `valueFn` against a **synthetic RF cursor** with placeholder columns, detects variant-ness, then synthesises one of `RelationalPropertyMapping` (non-variant), `SemiStructuredEmbeddedRelationalInstanceSetImplementation` (variant + Class or Binding), or `SemiStructuredRelationalPropertyMapping` (variant + primitive/Variant). Placeholder TACs are resolved against the outer source at column-navigation time. |
| How do local properties differ from class properties? | Local properties are declared with `+name: Type[mult]` in the mapping and exist only in the mapping scope — they do not modify the canonical Pure class. |
| Round-tripping — will my bare-column authoring survive? | The protocol always emits `valueFn`. The composer will render the lowered `$src.<col>` form on the round-trip. Semantics are preserved; the surface syntax may change from `col` to `$src.col`. |

---

## 14. Authoritative File Map

| Concern | Key files |
|---------|-----------|
| Lexer / Parser grammars | `RelationFunctionMappingLexerGrammar.g4`, `RelationFunctionMappingParserGrammar.g4` |
| Parse-tree walker | `RelationFunctionMappingParseTreeWalker.java` |
| Grammar composer | `DEPRECATED_PureGrammarComposerCore.java` |
| Protocol POJOs | `RelationFunctionClassMapping.java`, `RelationFunctionPropertyMapping.java`, `RelationFunctionEmbeddedPropertyMapping.java` |
| Compiler — prerequisite / first / second / third passes | `ClassMappingPrerequisiteElementsPassBuilder.java`, `ClassMappingFirstPassBuilder.java`, `ClassMappingSecondPassBuilder.java`, `ClassMappingThirdPassBuilder.java` |
| Compiler — property mappings | `PropertyMappingBuilder.java` |
| Compiler — bare-column matcher | `RelationFunctionPropertyMappingTools.java` |
| Compiler — validation | `MappingValidator.java` |
| Primary-key inference (Pure) | `core/pure/mapping/relationFunctionMapping.pure` |
| Runtime helpers / PK synthesis | `core_relational/relational/helperFunctions/helperFunctions.pure` |
| SQL metamodel additions | `core_relational/relational/pureToSQLQuery/metamodel.pure` (`RelationFunction`, `RelationFunctionColumn`) |
| Main SQL generation | `core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure` |
| Variant / semi-structured SQL generation | `core_relational/relational/pureToSQLQuery/pureToSQLQuery_variant.pure` |
| Union SQL generation | `core_relational/relational/pureToSQLQuery/pureToSQLQuery_union.pure` |
| Routing / store contract | `core/pure/router/store/cluster.pure`, `core/pure/router/store/routing.pure` |
| Inline-embedded resolution | `core/pure/mapping/mappingExtension.pure` |
| Protocol transfer (Pure ↔ engine) | `core/pure/protocol/vX_X_X/models/dsl/mapping.pure`, `core/pure/protocol/vX_X_X/transfers/mapping.pure` |

---

## 15. Quick Reference: Call Graph

```
PARSE
  CorePureGrammarParser.parseRelationFunctionClassMapping
  └── RelationFunctionMappingParseTreeWalker.visitRelationFunctionClassMapping
        ├── relationSource:
        │     ├── RELATION_FUNC → PackageableElementPointer
        │     └── RELATION_SRC  → visitInlineExpressionAsLambda → LambdaFunction (body=[expr], parameters=[])
        ├── primaryKey[] parsed from context (empty if omitted)
        └── visitPropertyMapping
              ├── visitRelationFunctionPropertyMapping
              │     ├── identifier    → propertyMapping.column
              │     └── combinedExpression → visitInlineExpressionAsLambda → propertyMapping.valueFn
              ├── visitRelationFunctionEmbeddedPropertyMapping    (normal, recursive)
              └── visitInlineRelationFunctionEmbeddedPropertyMapping  (inline, id only)

COMPILE (4 passes)
  ClassMappingPrerequisiteElementsPassBuilder.visit(RelationFunctionClassMapping)
  └── declares CLASS + (~func only) FUNCTION as prerequisites

  ClassMappingFirstPassBuilder.visit(RelationFunctionClassMapping)
  └── creates RelationFunctionInstanceSetImplementation
      └── PropertyMappingBuilder.visit(RelationFunctionPropertyMapping)
            (skeleton only; _valueFn filled at SecondPass)
      └── PropertyMappingBuilder.visit(RelationFunctionEmbeddedPropertyMapping)
            ├── creates EmbeddedRelationFunctionSetImplementation
            ├── rewrites sub-property `_class` pointers to embedded target class
            └── recursively compiles inner propertyMappings

  ClassMappingSecondPassBuilder.visit(RelationFunctionClassMapping)
  ├── resolves ~func by descriptor OR compiles ~src inline lambda
  ├── attaches to setImpl._relationFunction
  ├── validates return type is Relation<...>
  ├── extracts srcType from last-expression RelationType
  └── buildValueFunctionsForPropertyMappings
        ├── for each RFPM: buildPropertyValueFn → compileRelationPropertyLambda → sets _valueFn
        └── for each EmbeddedRFSet: propagates relationFunction; recurses

  ClassMappingThirdPassBuilder.visit(RelationFunctionClassMapping)
  └── resolves explicit ~primaryKey names against RelationType
        └── getRelationFunctionColumns (reads last-expression type args)

VALIDATE (MappingValidator.validateRelationFunctionClassMapping)
  ├── inline embedded id → exists as RelationFunctionClassMapping in same protocol Mapping
  └── each RelationFunctionInstanceSetImplementation:
        ├── zero-parameter function
        ├── return type is Relation<...>
        └── validateRelationFunctionPropertyMapping (recursive):
              ├── body multiplicity subsumed by property multiplicity
              └── body raw type subtype of property raw type (skipped when transformer present)

ROUTE (cluster.pure, routing.pure)
  storeContractForSetImplementation
    ├── RelationFunctionInstanceSetImplementation → store from routed function
    ├── EmbeddedSetImplementation                 → delegate to owner
    └── OperationSetImplementation (union)        → resolve per leaf; single-store enforced

  potentiallyRouteRelationFunctionSet (single-set)
    ├── RelationFunctionInstanceSetImplementation → routes .relationFunction
    ├── EmbeddedSetImplementation / InstanceSetImplementation → pass-through
    └── OperationSetImplementation → recurse into leaves

  potentiallyRouteRelationFunctionSets (class-level, called from processClass / processProperty)
    ├── partitions RF vs non-RF sets
    ├── routes unrouted RF sets via routeFunction; consults classMappingsByClass cache
    ├── recurses OperationSetImplementation and EmbeddedRelationFunctionSetImplementation
    └── updates classMappingsByClass cache

PK INFERENCE (relationFunctionMapping.pure)
  resolveRelationFunctionPrimaryKey
    ├── explicit column names → intersect with RelationType columns
    └── auto-infer → inferPrimaryKeyColumnNames on last expression
          ├── InstanceValue                → all RelationElementAccessorExtension resolvers
          ├── ClusteredValueSpecification  → recurse into .val
          └── SimpleFunctionExpression     → inferPrimaryKeyColumnNamesFromFunctionExpression
                (per-operator table: filter/limit/sort/…, select/rename, groupBy, join, etc.)

SQL GENERATION (pureToSQLQuery.pure, pureToSQLQuery_variant.pure, pureToSQLQuery_union.pure)
  processRelationFunctionClassMapping
    ├── ensureRelationFunctionPrimaryKeyResolved (auto-infer PK if empty)
    ├── potentiallyRouteRelationFunctionSet
    ├── evaluate expressionSequence → ClusteredValueSpecification.val
    ├── processValueSpecification against fresh SelectWithCursor
    └── moveSelectQueryToSubSelect → wrapped sub-select

  processGetAll
    ├── RootRelationalInstanceSetImplementation → processGetAll
    ├── RelationFunctionInstanceSetImplementation → processRelationFunctionClassMapping
    └── OperationSetImplementation → single-leaf dispatch OR buildUnion

  transformRelationFunctionPropertyMappingToRelational   (RFPM → downstream PM)
    ├── evaluateRfpmValueFn
    │     ├── buildSyntheticRfCursor (placeholder RelationFunctionColumns)
    │     ├── bind $src via updateFunctionParamScope
    │     ├── processValueSpecification against synthetic cursor
    │     └── expressionTouchesVariant → RfpmValueFnResult
    └── dispatch:
          ├── Binding                    → SemiStructuredEmbeddedRelationalInstanceSetImplementation
          ├── non-variant valueFn        → RelationalPropertyMapping
          ├── variant + Class target     → SemiStructuredEmbeddedRelationalInstanceSetImplementation
          └── variant + primitive target → SemiStructuredRelationalPropertyMapping

  resolveTableAliasColumn (invoked during column navigation)
    └── detects placeholder RelationFunctionColumn (owner empty) → reuses or adds to outer select

  buildUnion (InstanceSetImplementation[*])
    ├── per-leaf: processGetAll (relational) OR processRelationFunctionClassMapping (RF)
    ├── milestoningColumns → [] for RF leaves
    ├── allColumns → synthesised from SelectSQLQuery.columns for RF leaves
    ├── findFkListForEachSet: RF alias-filter for TableAliasColumns
    ├── isSameRelation: RelationFunction equality by .owner
    ├── buildUniqueName: `rf(<id>)` arm
    └── findUnionPropertyMapping: RF + Embedded RF arms
```
