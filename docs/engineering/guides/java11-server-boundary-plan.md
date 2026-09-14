# Java 11 Server Boundary — Upgrade Plan

**Owner:** Legend Engine core team
**Related:** [ADR-003: Java 11 Server Boundary](../decisions/ADR-003-java-11-server-boundary.md) ·
[Handoff — current state](java11-server-boundary-handoff.md)

> For *where the work currently stands* — which commits exist, what is verified, and where the
> build stops — read the [Handoff](java11-server-boundary-handoff.md) first. This document is the
> reference for the shape of the work and the reasoning behind it.

---

## Context

Five vulnerability findings against `finos-legend-engine` all trace back to the HTTP serving stack:

| Vulnerable package | Recommended |
|---|---|
| `org.eclipse.jetty:jetty-http:9.4.44.v20210927` | `jetty-http` ≥ 10.0.0 |
| `org.eclipse.jetty:jetty-http:9.4.57.v20241219` | `jetty-http` ≥ 10.0.0 |
| `org.eclipse.jetty:jetty-security:9.4.57.v20241219` | `jetty-security` 12.0.36 |
| `org.eclipse.jetty:jetty-client:9.4.57.v20241219` | `jetty-client` 12.0.36 |
| `com.fasterxml.jackson.core:jackson-databind:2.10.5` | `jackson-databind` 2.18.8 |

The blocker is **Dropwizard 1.3.29**, which pins Jetty 9.4 and cannot be upgraded without a Java 11
compile target. At the same time downstream clients — DataLake refiners, Studio, `pure-model`
consumers — still require **Java 1.8 bytecode** from this repo, so the whole reactor cannot simply
move to 11.

The approach: draw an **enforced, self-documenting Java 8 / Java 11 line** through the reactor, put
everything that serves HTTP on the Java 11 side, then upgrade Dropwizard/Jetty/Jackson behind that
line. This is iteration one of a pattern to be repeated in `legend-shared`, `legend-pure`, SDLC,
metadata and Alloy.

## What the codebase actually looked like

Measured, not assumed:

1. **`maven.compiler.release=8` in the root pom silently overrode every module-level
   `source`/`target`.** `maven-compiler-plugin` binds `release` to the `maven.compiler.release`
   property, and when `release` is set the plugin ignores `source`/`target`. 31 modules declared
   `<maven.compiler.source>11</maven.compiler.source>` — every one of them emitted major 52. Dead,
   misleading configuration.
2. **The Dropwizard/Jetty blast radius was tiny.** Exactly six modules carried Dropwizard or Jetty
   on a compile/runtime classpath. `extensions-collection-generation`,
   `extensions-collection-execution` and every REPL module were already free of both.
3. **Only two wrong-direction dependency edges** blocked putting all `*-http-api` / `*-api` modules
   on Java 11.
4. **`legend-shared` 0.37.x is compiled against the pre-3.0 Dropwizard package layout**, so the
   engine cannot move to DW 3.0 alone.

---

## Step 1 — Parameterise the bytecode enforcement

Root `pom.xml`:

- `<maxJdkVersion>1.8</maxJdkVersion>` → `<maxJdkVersion>${maven.compiler.target}</maxJdkVersion>`
  in the `enforce-bytecode-version` execution. `extra-enforcer-rules` accepts both `1.8` and `11`,
  so a module that raises its target automatically raises its own ceiling and every module still on
  1.8 keeps rejecting Java 11 dependencies.
- Add `<ignoredScopes><ignoredScope>test</ignoredScope></ignoredScopes>`. Test bytecode never ships
  to clients and both CI and the supported dev JDKs are 11+.
- Make `release` explicit in the `maven-compiler-plugin` configuration next to `source`/`target`, so
  the governing knob is visible rather than implied by a property default.

Then **delete the 31 inert `source`/`target` overrides** so each pom tells the truth about what it
emits, and fix `legend-engine-xt-dataquality-api`, which declared `dropwizard-testing` at compile
scope.

## Step 2 — Draw the boundary

48 modules move to Java 11 (all three of `source`, `target`, `release`):

| Group | Modules |
|---|---|
| Dropwizard / Jetty | `legend-engine-server-support-core`, `legend-engine-server-http-server`, `legend-engine-server-integration-tests`, `legend-engine-pure-ide-light-http-server`, `legend-engine-test-server-shared`, `legend-engine-xt-sql-postgres-server` |
| REST layer | all 35 `*-http-api` modules and 7 `*-api` modules |

Each also pins `maven-dependency-plugin` to 3.1.2 — 2.10 bundles an ASM that cannot read major-55
class files.

### Two couplings broken first

**(a) `legend-engine-xt-hostedService-protocol` → `legend-engine-xt-analytics-lineage-http-api`.**
A protocol module depending on an HTTP API module is backwards regardless of this effort. One file
(`SingleLineage.java`) imported five plain POJOs.

*Fix:* new Java 8 module `legend-engine-xt-analytics-lineage-protocol` holding the whole
`org.finos.legend.engine.api.analytics.model.**` tree. Only the JAX-RS resource
`LineageAnalytics.java` stays in `-http-api`. Clears `hostedService-protocol`, `-compiler`,
`-generation` and `-grammar` in one move.

**(b) `legend-engine-extensions-collection-generation` → `legend-engine-xt-graphQL-http-api`.**
That module held 30 classes of which only three were JAX-RS resources (`GraphQLExecute`,
`GraphQLDebug`, `GraphQLGrammar`). The other 27 — the `GenerationExtension` /
`ExternalFormatExtension` SPI implementations that `collection-generation` actually wants, plus
caches, directives and model/error POJOs — are ordinary library code.

*Fix:* new Java 8 module `legend-engine-xt-graphQL-api-core` holding those 27 classes and the three
`META-INF/services/` registration files. The static helper `GraphQL.toPureModel` moved to
`GraphQLExecutionHelper` so `TotalCountDirective` no longer reaches through a REST resource to get
at it. `collection-generation` and `graphQL-relational-extension` now depend on `-api-core`;
the latter keeps a `test`-scoped dependency on `-http-api`.

## Step 3a — Jackson

`jackson.version` and `jackson.databind.version` collapse to a single value at **2.19.4**.

Do not assume Jackson 2.x is uniformly Java 8. Individual artifacts have shipped mis-targeted:
`jackson-dataformat-xml:2.18.10` is major **61** and `jackson-databind:2.19.3` is major **55**.
**Verify class-file majors before bumping.** 2.19.4 is clean across all ten artifacts this repo
uses.

`javax.xml.bind:jaxb-api` needs pinning to 2.3.1 — `jackson-module-jaxb-annotations` pulls 2.2.12
while Dropwizard pulls 2.3.1, and `dependencyConvergence` is enforced.

Watch `StreamReadConstraints` (Jackson 2.15+): a 20 MB string cap, 1000 nesting depth and a name
length cap now apply by default. Legend routinely moves very large `PureModelContextData` payloads.
Where limits bite, raise them on the mapper rather than reverting the bump.

## Step 3b — Dropwizard 3.0.17 and Jetty 10.0.26

Dropwizard 3.0.17 is the right target: Java 11, Jetty 10.0.26, Jersey 2.47, and
`jakarta.ws.rs-api` 2.1.6 — which is still the `javax.*` package names. DW 2.1.12 is Java 8 but
ships Jetty 9.4.53 and does not clear the finding; DW 4.0 forces the `jakarta.*` migration;
DW 5.0 requires Java 17.

### The pac4j chain

DW 3.0 is not reachable by version bumps alone:

```
dropwizard 1.3.29 -> 3.0.17
  requires org.pac4j:dropwizard-pac4j 4.0.0 -> 5.3.0   (4.0.0 is built against DW 1.3.25)
    requires pac4j 4.5.8 -> 5.7.1                      (5.3.0 is built against pac4j 5.7.1)
```

There is no `dropwizard-pac4j` pairing DW 3.x with pac4j 4.x — the matrix is 4.0.0→DW 1.3,
5.0.0→DW 2.0, 5.3.0→DW 3.0, 6.0.0→DW 4.0, 7.0.0/8.0.0→DW 5.

pac4j 5 deletes the exact extension points `legend-shared-pac4j` is built on:
`ProfileStorageDecision`, `AlwaysUseSessionProfileStorageDecision`, `JavaSerializationHelper`,
`JEEContext` and `JEESessionStore` are all gone, and `SessionStore` / `ProfileManager` lose their
generics. Swapping `JavaSerializationHelper` for `JavaSerializer` would also change the on-disk
format of persisted sessions.

**Decision: vendor the Dropwizard glue instead.** `org.pac4j:dropwizard-pac4j` 4.0.0 is 1,133 lines
across 16 classes and its entire Dropwizard coupling is seven imports, six of which are straight
package moves under DW 3.0 (the seventh, `Bundle`, collapses into `ConfiguredBundle`). legend-shared
consumes only `Pac4jBundle`, `Pac4jFactory` and `Pac4jFeatureSupport`. Copying those classes into a
new `legend-shared-pac4j-dropwizard` module gets DW 3.0.17 + Jetty 10.0.26 with pac4j untouched —
48 legend-shared files and 175 engine `ProfileManager<CommonProfile>` sites stay as they are, and
there is no session-format risk. See ADR-003 for the full rationale.

### Work items

**`finos-legend-shared`**
1. New `legend-shared-pac4j-dropwizard` module with the vendored classes on DW 3.0 imports; drop
   `org.pac4j:dropwizard-pac4j` from `legend-shared-pac4j`, `-pac4j-kerberos` and `-pac4j-ping`.
2. Root pom: dropwizard → 3.0.17, jetty → 10.0.26, jackson → 2.19.4,
   `maven.dependency.plugin.version` → 3.1.2.
3. `maven.compiler.release` → 11 for `legend-shared-server` and `legend-shared-pac4j` only — the
   two modules with DW/Jetty imports. Everything else stays on 8;
   `legend-shared-pac4j-kerberos` in particular is consumed by Java 8 engine modules.
4. Rewrite the DW imports to `io.dropwizard.core.*` (11 `Environment`, 10 `Bootstrap`,
   6 `Configuration`, 3 `ConfiguredBundle`, 3 `Bundle`, 3 `Application`, 1 `SimpleServerFactory`).
   `configuration.*`, `servlets.assets.*` and `testing.ResourceHelpers` keep their packages.
5. 2 `DropwizardAppRule` usages → `junit5.DropwizardAppExtension`.
6. Jetty needs no source change — every imported type exists in Jetty 10.

**`finos-legend-engine`** (only after legend-shared installs cleanly)

| property | from | to |
|---|---|---|
| `dropwizard.version` | 1.3.29 | 3.0.17 |
| `dropwizard-swagger.version` | 1.3.17-1 | 3.0.0-1 |
| `jetty.version` | 9.4.44.v20210927 | 10.0.26 |
| `jersey.version` | 2.25.1 | 2.47 |
| `dropwizard.metrics.version` | 4.1.16 | 4.2.38 |
| `swagger.annotation.version` | `io.swagger:swagger-annotations` 1.5.20 | `io.swagger.core.v3:swagger-annotations` 2.2.x |
| `legend.shared.version` | 0.37.0 | 0.37.1-SNAPSHOT |

- ~48 `io.dropwizard.*` imports across 8 modules move to the DW 3 layout.
- 11 `io.dropwizard.testing.junit.ResourceTestRule` usages → `junit5.ResourceExtension`; DW 3.0
  dropped JUnit 4 support entirely. This is the direction `CLAUDE.md` already mandates.
- Swagger annotations in 82 files: `Api` → `tags.Tag`, `ApiOperation` → `Operation`,
  `ApiParam` → `Parameter`, `ApiModelProperty` → `media.Schema`, because
  `com.smoketurner:dropwizard-swagger:3.0.0-1` scans with `swagger-jaxrs2`. The bundle keeps the
  `io.federecio.dropwizard.swagger.SwaggerBundle` class names, so `Server.java`, `BaseServer.java`
  and `ServerShared.registerSwagger` need no structural change. The published spec becomes
  OpenAPI 3 — confirm nothing downstream consumes the Swagger 1.2 shape at `/api/swagger`.
- Jetty needs no source change; `HandlerCollection` is deprecated in 10, worth noting for the
  eventual Jetty 12 move.

---

## Build discipline

A full-reactor legend-engine build **crashed a 32 GB VM** when exploration ran alongside it.

- One Maven process at a time, always `-T 1`. Nothing else runs while a reactor build is in flight.
- One named module per `-pl`; no `-am`, no multi-module `-pl` lists.
- **Do not pass `-Drevision`.** The baseline install is at the default `4.145.1-SNAPSHOT`;
  overriding the revision makes single-module builds fail to resolve already-installed siblings and
  forces a multi-hour `-am` rebuild. This is a deliberate departure from the `-Drevision` isolation
  advice in `CLAUDE.md`, which assumes a from-scratch build.
- `legend-engine-pure-ide-light-http-server` needs
  `-Dnpm.registry.url=https://npm.aws.site.gs.com/repository/npm-group`.
- After editing the root pom, run `mvn -N -DskipTests install` so modules resolving the parent from
  the repository see the new `dependencyManagement`.

Order: **legend-shared alone → verify → legend-engine alone → verify.**

## Verification

Green so far:

- **Step 1** — whole-reactor `mvn -T 1 validate` passes across all 604 modules. Falsification test:
  `-Dmaven.compiler.target=1.7` on `legend-engine-shared-core` makes `enforce-bytecode-version`
  fail, proving the property is actually read rather than silently defaulted.
- **Step 2** — all 48 Java 11 modules build. Bytecode audit: 47/48 at major 55 (the 48th is
  test-only and its `test-classes` are 55), 418 Java 8 modules at major 52, **0 mismatches**.
  graphQL suite green (71 + 5 tests) after the split.
- **Step 3a** — whole-reactor `validate` passes on Jackson 2.19.4; 927 tests green across
  `legend-engine-shared-core`, `legend-engine-protocol-pure`, `legend-engine-language-pure-grammar`
  and `legend-engine-language-pure-compiler`.

Outstanding:

1. Full `mvn clean install -DskipTests -T 1` on legend-engine as the gate for steps 1+2+3a (~3 h,
   nothing else running).
2. legend-shared `mvn clean install -T 1` with tests, after the Step 3b changes.
3. legend-engine against the new legend-shared SNAPSHOT, then the server end to end:
   ```
   server legend-engine-config/legend-engine-server/legend-engine-server-http-server/src/test/resources/org/finos/legend/engine/server/test/userTestConfig.json
   ```
   Confirm <http://127.0.0.1:6300/api/swagger> renders, then round-trip
   `/grammar/transformGrammarToJson`, `/compilation/compile`, `/executionPlan/generate` and
   `/execution/execute`. Start `PureIDELight` and confirm the IDE still serves and compiles.
4. Exercise an authenticated path — the vendored `Pac4jBundle` is the highest-risk piece of Step 3b
   and unit tests do not cover bundle wiring.
5. `mvn checkstyle:check` in both repos; new files need the Apache 2.0 header.

## Out of scope

- **pac4j 4.5.8 → 5.7.x.** Deferred by vendoring the Dropwizard glue. It is a separate piece of work
  with its own session-serialization migration, and DW 4/5 will eventually force it.
- `legend-pure`, SDLC, metadata, Alloy — same treatment, later iterations.
- **Jetty 12 / Dropwizard 5.** Requires Java 17 and the `jakarta.*` migration. Jetty 10.0.26 and
  11.0.26 are the final releases of those lines, so revisit when Jetty 10's EOL becomes a finding.
- **Bumping `maven-dependency-plugin` globally.** It is overridden to 3.1.2 only in the 48 Java 11
  modules. 3.3.0+ adds a stricter analyzer that flags pre-existing pom hygiene in ~30 modules — a
  worthwhile but separate cleanup.
- The Studio-side client dependency list, which lives in `legend-studio`.
