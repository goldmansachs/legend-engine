# ADR-003: A Java 11 Compile Boundary for Server Modules

**Status:** Accepted
**Date:** 2026-09-10
**Deciders:** Legend Engine core team

---

## Context

Five vulnerability findings against `finos-legend-engine` resolve to two dependencies: Jetty 9.4.x
and `jackson-databind` 2.10.5. Jetty cannot move off 9.4 while the server runs Dropwizard 1.3.29,
and no Dropwizard release that ships Jetty 10 or later targets Java 8.

Meanwhile the repository publishes artifacts that downstream clients consume on a Java 8 runtime —
DataLake refiners, Studio, and the `pure-model` family. Raising the whole reactor to Java 11 would
break them.

The decision is therefore not "should we upgrade Dropwizard" but "where does the Java 8 contract
actually end". Everything below follows from drawing that line explicitly and enforcing it in the
build rather than leaving it to convention.

## Decision

Introduce an enforced Java 8 / Java 11 boundary. The 48 modules that serve HTTP compile to Java 11;
the remaining ~556 stay on Java 8, and the Maven enforcer fails the build if a Java 8 module ever
acquires a Java 11 dependency. Dropwizard, Jetty and Jackson are then upgraded behind that line.

Full implementation detail lives in
[the upgrade plan](../guides/java11-server-boundary-plan.md).

## Consequences

**Positive.** The client/server split becomes a build-enforced invariant instead of a convention,
and it self-maintains: because the enforcer ceiling tracks each module's own compile target, moving
a module to Java 11 automatically raises only its own ceiling. Two long-standing wrong-direction
dependency edges were removed as a side effect. The Jackson finding is cleared independently of the
Java 11 work.

**Negative.** Two new modules exist purely to break dependency cycles. 48 poms carry compiler
properties that the root pom would otherwise supply. `legend-shared` must now be released in
lockstep for any Dropwizard change. And the repository carries a vendored copy of
`dropwizard-pac4j` (see D-13), which must be re-synced if upstream fixes anything material.

**Deferred.** pac4j stays on 4.5.8, and Jetty on the 10.x line whose final release has already
shipped. Both will be forced eventually by Dropwizard 4/5, which additionally require the
`jakarta.*` migration and Java 17.

---

## Decision log

Every decision taken during this work, with the evidence that drove it. "Evidence" means something
measured in the repo or in published artifacts, not a judgement call, unless stated otherwise.

### Enforcement

| # | Decision | What forced it |
|---|---|---|
| D-1 | Parameterise the enforcer ceiling as `${maven.compiler.target}` rather than hardcoding `1.8` or `11` | Direct request. A hardcoded ceiling has to be edited in lockstep with every module that changes target; tracking the property means the two can never drift. `extra-enforcer-rules` accepts both `1.8` and `11` spellings. Verified to actually bite: forcing `-Dmaven.compiler.target=1.7` on `legend-engine-shared-core` makes `enforce-bytecode-version` fail with a list of banned dependencies. |
| D-2 | Make `<release>` explicit in the compiler plugin configuration | `maven.compiler.release=8` was set as a property while the plugin configuration listed only `source`/`target`. Since `release` wins when set, the visible configuration was not the effective one. |
| D-3 | Delete the 31 module-level `source`/`target=11` overrides rather than honour them | Class-file headers in `target/classes` showed all 31 emitting major 52 — the root `release=8` had been overriding them silently. Honouring them would have moved modules like `legend-engine-identity-core` (a `collection-generation` dependency) to Java 11 and broken the client contract. Deleting is a no-op on output and makes each pom truthful. |
| D-4 | Add `<ignoredScopes>test</ignoredScopes>` to the bytecode rule | Java 11 test-only jars (`dropwizard-testing` 3.x, Jetty 10 in `graphQL-relational-extension` tests) would otherwise fail modules that are legitimately Java 8 at runtime. Test bytecode is never published, and CI and dev both run JDK 17. |
| D-5 | Move `dropwizard-testing` to `test` scope in `legend-engine-xt-dataquality-api` | It was declared at compile scope — a plain mistake that put a Dropwizard jar on a published classpath. |

### The boundary

| # | Decision | What forced it |
|---|---|---|
| D-6 | Put all 35 `*-http-api` and 7 `*-api` modules on Java 11, not only the 6 with Dropwizard/Jetty | User choice between a minimal set and the "implements REST APIs" heuristic. The wider line documents the client/server split more honestly; a reverse-dependency scan showed the cost was only two edges to break. |
| D-7 | Extract `legend-engine-xt-analytics-lineage-protocol` | `legend-engine-xt-hostedService-protocol` imported five plain POJOs from `-lineage-http-api`. A protocol module depending on an HTTP API module is backwards regardless of this effort; the module only contains one JAX-RS resource, so splitting the POJOs out is the natural fix. Clears four dependent modules at once. |
| D-8 | Extract `legend-engine-xt-graphQL-api-core` | `collection-generation` (explicitly client-facing) declared `-graphQL-http-api` directly. Of that module's 30 classes only three are JAX-RS resources; the SPI `GenerationExtension` / `ExternalFormatExtension` implementations `collection-generation` actually needs are ordinary library code. |
| D-9 | Move the static `GraphQL.toPureModel` into `GraphQLExecutionHelper` | Discovered mid-split: `TotalCountDirective` in `graphQL-relational-extension` called `GraphQLExecute.toPureModel(...)`, reaching an inherited static through a REST resource. Relocating it to the helper (already in `-api-core`) removed the last main-source edge. An initial grep was truncated by `head` and missed this — the compiler caught it. |
| D-10 | Keep `graphQL-relational-extension`'s `test`-scoped dependency on `-http-api` | Its tests construct a real `GraphQLExecute`. Legal under D-4 and cheaper than restructuring the test. |
| D-11 | Pin `maven-dependency-plugin` to 3.1.2 in the 48 Java 11 modules only | 2.10's bundled ASM throws `IllegalArgumentException` on major-55 class files, failing all 48. Probing 3.8.1 and 3.3.0 showed their stricter analyzer ("Non-test scoped test only dependencies") flags pre-existing pom hygiene in ~30 modules — unrelated churn. 3.1.2 reads Java 11 bytecode with analyzer behaviour identical to 2.10. |
| D-12 | Use `ignoredUsedUndeclaredDependencies` for `google-cloud-core` in `bigqueryFunction-api` rather than declaring it | 3.1.2 newly detected the transitive use. Declaring it directly pulled a second `google-http-client` line (1.42.3 vs 1.43.3) and broke `dependencyConvergence`. The ignore keeps the resolved dependency graph byte-identical in a module we are not otherwise changing. Three genuinely missing declarations (`jersey-test-framework-core` ×2, `org.postgresql:postgresql`) were added properly. |

### Dependency upgrades

| # | Decision | What forced it |
|---|---|---|
| ~~D-13~~ | ~~Vendor `org.pac4j:dropwizard-pac4j` into legend-shared instead of upgrading pac4j 4.5.8 → 5.7.1~~ **SUPERSEDED by D-23** | Reasoning at the time: no `dropwizard-pac4j` pairs DW 3.x with pac4j 4.x, and pac4j 5 deletes the extension points `legend-shared-pac4j` is built on, so vendoring the 1,133-line Dropwizard shim looked like the cheap way out. **This was wrong** — it checked only the Dropwizard coupling, not the Jersey one. See D-23. |
| D-23 | Do the full pac4j 4.5.8 → 5.7.1 upgrade after all; discard the vendored module and use `org.pac4j:dropwizard-pac4j:5.3.0` | The vendored build compiled but failed at runtime with `NoClassDefFoundError: org/glassfish/jersey/server/internal/inject/AbstractContainerRequestValueFactory`. Root cause: the only pac4j-4-compatible Jersey module is `jersey225-pac4j`, built against Jersey **2.25**, and that class was removed in Jersey 2.26. Dropwizard 1.3 is the last release shipping Jersey 2.25 (DW 2.x ships 2.41, DW 3.0.17 ships 2.47), so **any Dropwizard upgrade past 1.3 forces pac4j 5**. The Jersey-2.26+ replacement (`jersey2-pac4j`) exists only from 5.0.0, which calls pac4j 5's two-arg `ProfileManager(WebContext, SessionStore)` constructor. Back-porting it was rejected: the diff between the two upstream `Pac4JValueFactoryProvider` versions is 240 lines against ~250 LOC — effectively a rewrite against Jersey's reworked injection SPI, in code neither repo tests. |
| D-24 | Treat the persisted-session format as **not** at risk | The concern raised in D-13 was checked rather than assumed: `javap -c` on pac4j 4.5.8's `JavaSerializationHelper` and pac4j 5.7.1's `JavaSerializer` shows both write plain `ObjectOutputStream` bytes over a `ByteArrayOutputStream`, with a restricted `ObjectInputStream` on read. The wire format is identical, so existing Mongo/Hazelcast sessions still deserialize provided the trusted-package list is carried over (it is). |
| D-25 | Keep `LegendUserProfileStorageDecision` as a plain helper rather than deleting it | pac4j 5 removed the `ProfileStorageDecision` SPI, but the Legend behaviour it encodes — session storage gated on the `@SerializableProfile` annotation — is a product decision, not a pac4j detail. Dropping `implements ProfileStorageDecision` and having `LegendSecurityLogic` call it directly preserves the behaviour and its unit tests. `AlwaysUseSessionProfileStorageDecision` became a local subclass for the same reason. |
| D-26 | Carry `multiProfile` as state on `LegendSecurityLogic` | pac4j 5 dropped the `inputMultiProfile` argument from `SecurityLogic.perform` and sources it per-client instead. legend-shared configures it once for the whole application, so a setter on the logic is the smallest faithful mapping. |
| D-27 | slf4j 1.7.36 → 2.0.17 and `javax.servlet:javax.servlet-api:3.1.0` → `jakarta.servlet:jakarta.servlet-api:4.0.4` | Both surfaced as runtime failures, not compile errors. DW 3.0 ships logback 1.3.16, which needs the slf4j 2.x binding — with 1.7 on the classpath every server test failed with "Unable to acquire the logger context". And DW 3.0's filters rely on `Filter#init`/`#destroy` being *default* methods, which they only are from Servlet 4.0; Servlet 3.1 gave `AbstractMethodError` on `AllowedMethodsFilter`. `jakarta.servlet-api` 4.0.4 still uses the `javax.servlet` package names, so this is not the jakarta migration. Both artifacts verified as Java 8 bytecode. |
| D-28 | Read cookies from the `Set-Cookie` header in session-store tests | pac4j 5's `JEEContext.addResponseCookie` writes a raw header instead of calling `HttpServletResponse.addCookie`, so Spring's `MockHttpServletResponse.getCookies()` stopped seeing them. The product behaviour is unchanged — only the mock's accounting — so the assertions were repointed at the header rather than the behaviour being altered. |
| D-14 | Target Dropwizard **3.0.17** | Fetched the DW dependency BOMs: 2.1.12 is Java 8 but ships Jetty 9.4.53 (does not clear the finding); 3.0.17 is Java 11, Jetty 10.0.26, Jersey 2.47 and `jakarta.ws.rs-api` 2.1.6 — still the `javax.*` package names, so the 35 http-api modules need no namespace migration; 4.0.17 is Jetty 11 with the real `jakarta.*` migration; 5.0.2 requires Java 17. |
| D-15 | Accept Jetty 10 despite two findings naming 12.0.36 | The five findings are internally inconsistent — `jetty-http` is listed as fixed at ≥10.0.0 while `jetty-security` and `jetty-client` name 12.0.36. Jetty 12 means DW 5 and Java 17, contradicting the Java 11 goal. Flagged to the user, who accepted Jetty 10. Noted as out of scope that 10.0.26/11.0.26 are the final releases of those lines. |
| D-16 | Jackson **2.19.4**, not the recommended 2.18.10 | Downloading and reading class-file headers showed `jackson-dataformat-xml:2.18.10` is major **61** (Java 17) and `jackson-databind:2.19.3` is major **55** — Jackson has shipped individually mis-targeted artifacts. 2.19.4 is major 52 across all ten artifacts this repo uses and satisfies the ≥2.18.8 recommendation. The lesson, recorded in the pom: verify majors, do not trust the line. |
| D-17 | Pin `javax.xml.bind:jaxb-api` to 2.3.1 | The Jackson bump surfaced a `dependencyConvergence` failure: `jackson-module-jaxb-annotations` 2.19.4 pulls 2.2.12 while `dropwizard-jersey` and `dropwizard-swagger` pull 2.3.1. Pinning the higher version in root `dependencyManagement` matches the repo's existing idiom. |
| D-18 | Sequence Jackson (3a) ahead of Dropwizard (3b) | Jackson is Java-8-safe and clears its finding independently, so it can land without waiting on the cross-repo Dropwizard work. |

### Java 11 transitives leaking into Java 8 modules

Both entries below are the same failure shape: the upgrade added a managed pin for the server's
benefit, and the pin — being global — landed a Java 11 jar on a Java 8 module's classpath. Neither
was visible before, because the pre-upgrade versions were all Java 8.

| # | Decision | What forced it |
|---|---|---|
| D-29 | Move `legend-engine-xt-serviceStore-executionPlan` to `wiremock-jre8-standalone`, and carve out its six shaded ALPN classes in the root enforcer rule | The module is Java 8, declares wiremock at **compile** scope (the only module that does) and uses it from five `src/main` files. With `org.eclipse.jetty:*` now pinned to 10.0.26, plain `wiremock-jre8` dragged major-55 Jetty onto its compile classpath. Marking the module Java 11 was ruled out: it is reachable from `legend-engine-extensions-collection-execution`, so that would break the Java 8 client contract. Moving the five classes to a test-support module was also ruled out — `ServiceStoreTestConnectionFactory` is registered as a production `ConnectionFactoryExtension` SPI and the two pattern generators as `ContentPatternToWiremockPatternGenerator` SPIs, so they ship deliberately and relocating them changes what the server discovers at runtime. The standalone jar was verified before adoption: **zero transitive dependencies**, Jetty shaded to `wiremock/org/eclipse/jetty/**`, and all eight API types the five files use present **unshaded**, so no source changed. One catch — its max class-file major is **53**, not 52: exactly six classes, the `JDK9*ALPNProcessor` pair Jetty loads reflectively for HTTP/2-over-TLS. Hence `<ignoreClasses>wiremock.org.eclipse.jetty.alpn.java.**</ignoreClasses>`. The carve-out lives in the **root** rule, not the module: the jar propagates at compile scope to `collection-execution` and `test-runner-service`, which failed identically when the ignore was module-local. It names one shaded package, so it cannot mask a Java 11 leak anywhere else. |
| D-30 | Pin caffeine back to 2.9.3 in `legend-engine-xt-iceberg-test-support` rather than globally or by moving the module | `dropwizard-dependencies:3.0.17` pins caffeine to 3.2.3, which is major 55, and the upgrade added a matching explicit pin in the root pom. `iceberg-core:1.3.0` declares caffeine **2.9.3** (major 52) at runtime scope, so the Java 8 test-support module inherited a Java 11 jar. No engine module uses caffeine directly — it is purely transitive, and only Dropwizard needs the 3.x line. Pinning 2.9.3 globally was rejected because DW 3.0.17 is compiled against caffeine 3 and the APIs differ. A module-level `dependencyManagement` override restores exactly the version iceberg was built against, keeps the Dropwizard pin confined to the Java 11 server modules, and changes no published bytecode. The module is a leaf — nothing in the reactor depends on it — so moving it to Java 11 would also have been safe, but it was unnecessary and would have changed an artifact's bytecode level for no gain. Module-level `dependencyManagement` is already an idiom here (`duckdb`, `sql-postgres-server`, `persistence-component`). |
| D-31 | Enumerate bytecode violations with a single `mvn validate -T 4 -fae` pass instead of one `install` build per failure | `enforce-bytecode-version` binds to `validate`, so the whole reactor can be swept in seconds without compiling. The first full build spent its time reaching one failure at a time; the sweep found the complete set at once. Caveat worth recording: run it **online**, or the two modules commit 1 split out (`-analytics-lineage-protocol`, `-graphQL-api-core`) fail to resolve for their dependents and produce four false positives — `validate` does not build them, and they have never been installed. |

### Behavioural changes the upgrade caused at runtime

None of these were compile errors. Each was found only by running the suite, and the last three were
invisible until the ones above them were fixed — the tests could not get far enough to reach them.

| # | Decision | What forced it |
|---|---|---|
| D-32 | Register `JavaTimeModule` on the mapper in `QueryModelConverter` | Jackson 2.19 enables `MapperFeature.REQUIRE_HANDLERS_FOR_JAVA8_TIMES` by default; 2.10.5 silently bean-serialised `java.time`. 75 `TestQueryStoreManager` tests failed on `LocalDateTime`. Checked for persisted-format risk before changing it: `Query` has no `audit` field and is `@JsonIgnoreProperties(ignoreUnknown = true)`, so the offending subtree is discarded, and the persistence layer (`BaseStoredVersionedAssetDao`) already registers `JavaTimeModule` — ISO-8601 was already the on-disk form. Disabling the feature was rejected: it would leave `@JsonFormat(Shape.STRING)` on `StoredAuditInformation` silently unhonoured. |
| D-33 | Move every wiremock consumer to `wiremock-jre8-standalone` | Slim `wiremock-jre8` loads `com.github.tomakehurst.wiremock.jetty9.JettyHttpServerFactory`, which needs `javax.servlet.DispatcherType`; the managed Jetty artifacts exclude `jetty-servlet-api`, so `TestSDLCLoader` died on `NoClassDefFoundError`. Supplying the servlet API would not have been enough — wiremock's Jetty 9 code would then run against the Jetty 10 jars the pin forces. The standalone jar shades both its Jetty and its servlet API, so it is immune. Applied to all four consumers, not just the one that failed: the other two that start a server passed only because their wiremock tests are `@Disabled`. |
| D-34 | Bump `logback.version` 1.2.3 → 1.3.16 | The upgrade set `slf4j` to 2.0.17 but left logback at 1.2.3, which only implements the slf4j 1.7 `StaticLoggerBinder`. Nothing then provides `SLF4JServiceProvider`, and `io.dropwizard.core.Application.bootstrapLogging` throws `IllegalStateException: Unable to acquire the logger context`. 1.3.16 is what DW 3.0.17 ships and is still Java 8 bytecode. The comment added next to the slf4j bump already said DW 3 needs logback 1.3 — only half the change was made. |
| D-35 | Drop the `org.javassist` exclusions and pin javassist to 3.30.2-GA | `hk2-locator` uses javassist for proxying. It was excluded from `hk2-locator`, `dropwizard-jersey` and `dropwizard-swagger`; Jersey 2.25 never reached the proxying path so nothing noticed, but Jersey 2.47 does and the server died starting up with `ClassNotFoundException: javassist.ClassPath`. The exclusions date to the initial commit with no recorded rationale. Pinning a current release rather than restoring hk2's 3.20.0-GA default keeps the old-version CVE exposure that probably motivated them from coming back. **Worth review**: this re-adds a dependency someone once deliberately removed. |
| D-36 | Enable `FromXmlParser.Feature.EMPTY_ELEMENT_AS_NULL` in the generated XML graph-fetch reader | Two M2M tests (`xmlSupportForEmptyValues`, `xmlSupportForReplacementUnsupportedCharactersInNames`) failed with the server returning HTTP 200 and a truncated body, because `DataParsingException: For input string: ""` was thrown *while streaming the response*. Verified by running the same parser code against both versions: Jackson **2.10.5** reports `<i/>` as `VALUE_NULL`, **2.19.4** reports `VALUE_STRING ""` — the empty string then reaches the generated primitive parsing. `EMPTY_ELEMENT_AS_NULL` (added in 2.12, default off) restores the 2.10.5 token exactly. Fixed in the generator (`graphFetchXml.pure`) rather than by teaching the generated code to tolerate `""`, so the parser behaviour matches what every existing mapping was written against. |

### Process

| # | Decision | What forced it |
|---|---|---|
| D-19 | Do **not** pass `-Drevision` | `CLAUDE.md` recommends it for cache isolation, but the baseline install is at the default `4.145.1-SNAPSHOT`. Overriding the revision makes single-module builds fail to resolve their already-installed siblings, forcing `-am` and a multi-hour rebuild. The isolation advice assumes a from-scratch build. |
| D-20 | Never run anything alongside a legend-engine reactor build | A full `mvn clean install -T 1` crashed the 32 GB VM while exploration ran concurrently. |
| D-21 | Build legend-shared to a local SNAPSHOT rather than waiting for a published release | The user cloned `finos-legend-shared` locally, which converted Step 3b from blocked-on-another-team to executable end to end. |
| D-22 | Do not write the un-compilable Dropwizard changes speculatively | Before legend-shared was available, `BaseServer`/`Server` extending `io.dropwizard.Application` and legend-shared's bundles implementing the pre-3.0 `ConfiguredBundle` meant a DW 3.0 branch could not compile, let alone be tested. Superseded by D-21. |
