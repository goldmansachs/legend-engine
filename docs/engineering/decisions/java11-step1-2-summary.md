# Java 8 / Java 11 Compile Boundary — Steps 1 & 2 (Enforcement and Identification)

**Repository:** `finos-legend-engine`
**Base commit:** `45c0c423c1c` (finos-master)
**Commit:** `453dae846f6` — single commit, 130 files, +693 / −170
**Status:** verified green, ships as a standalone PR independent of the Dropwizard upgrade

---

## 1. Why this work exists

Five vulnerability findings against `finos-legend-engine` all resolve to the HTTP serving stack:
Jetty 9.4.x and `jackson-databind` 2.10.5. Jetty cannot move off 9.4 while the server runs
Dropwizard 1.3.29, and **no Dropwizard release that ships Jetty 10 or later targets Java 8**.

At the same time the repository publishes artifacts that downstream clients consume on a **Java 8
runtime** — DataLake refiners, Studio, and the `pure-model` family. Moving the whole reactor to
Java 11 would break them.

So the question was never "should we upgrade Dropwizard". It was **"where does the Java 8 contract
actually end?"** Steps 1 and 2 answer that question and make the answer enforceable. The dependency
upgrade itself is separate work that sits behind this line.

---

## 2. Headline outcome

| | Count |
|---|---|
| Total reactor modules | 605 |
| Moved to **Java 11** | **48** (8%) |
| Remain on **Java 8** | **557** (92%) |
| New modules created to break dependency cycles | 2 |
| Boundary violations after the change | **0** |

The critical result: **the two client-facing aggregates cannot reach a single Java 11 module.**

| Client aggregate | Modules it reaches | Java 11 among them |
|---|---|---|
| `legend-engine-extensions-collection-execution` | 105 | **0** |
| `legend-engine-extensions-collection-generation` | 269 | **0** |

---

## 3. What we discovered before changing anything

These were measured in the repository, not assumed. Three of the four were surprises.

### 3a. The build was silently lying about its own Java version

**28 module poms declared `<maven.compiler.source>11</maven.compiler.source>` and
`<maven.compiler.target>11</maven.compiler.target>` — and every one of them was emitting Java 8
bytecode.**

The root pom set `maven.compiler.release=8` as a property. When `release` is set, the compiler
plugin **ignores `source`/`target` entirely**. Worse, the root's plugin configuration listed only
`<source>` and `<target>`, so the visible configuration was not the effective one. Class-file
headers in `target/classes` confirmed all 28 were major 52 (Java 8).

This mattered more than it looks. Had anyone "fixed" the build by honouring those declarations,
modules such as `legend-engine-identity-core` — a **`collection-generation` dependency** — would
have silently become Java 11 and broken the client contract. The dead configuration was an
accident waiting to happen.

**Action:** 25 of the 28 were demoted to plain Java 8 so the pom tells the truth (a no-op on emitted
bytecode); 3 were genuinely part of the server set and kept. `<release>` was made explicit in the
compiler configuration so the governing knob is visible.

### 3b. The Dropwizard/Jetty blast radius was far smaller than feared

Only **six** modules carried Dropwizard or Jetty on a compile/runtime classpath. Crucially,
`extensions-collection-generation`, `extensions-collection-execution` and every REPL module were
**already free of both**. The client-facing surface was never entangled with the HTTP stack — which
is what made a narrow boundary viable at all.

### 3c. Only two dependency edges pointed the wrong way

A reverse-dependency scan over all 605 modules found exactly **two** edges preventing the whole
`*-http-api` / `*-api` layer from moving. Both were pre-existing architectural defects, independent
of this effort. See §5.

### 3d. `legend-shared` blocks the upgrade, not the boundary

`legend-shared` 0.37.x is compiled against the pre-3.0 Dropwizard package layout, so the engine
cannot move to Dropwizard 3.0 alone. This constrains Step 3, **not** Steps 1–2 — which is precisely
why this patch can ship on its own.

---

## 4. What moved to Java 11 — and why that set

We chose the **"implements a REST API" heuristic** over the minimal six-module set. This was a
deliberate trade: the wider line documents the client/server split honestly rather than drawing it
at whatever happened to import Jetty today. The reverse-dependency scan showed the extra cost was
only the two edges in §5, so the wider line was nearly free.

**48 modules, in three groups:**

| Group | Count | Examples |
|---|---|---|
| REST layer — all `*-http-api` modules | 35 | `legend-engine-xt-sql-http-api`, `legend-engine-xt-graphQL-http-api`, `legend-engine-language-pure-grammar-http-api` |
| REST layer — `*-api` modules | 7 | `legend-engine-xt-dataquality-api`, `legend-engine-xt-snowflake-api`, `legend-engine-xt-hostedService-api`, `legend-engine-xt-functionJar-api` |
| Server / Dropwizard / Jetty hosts | 6 | `legend-engine-server-http-server`, `legend-engine-server-support-core`, `legend-engine-pure-ide-light-http-server`, `legend-engine-xt-sql-postgres-server`, `legend-engine-server-integration-tests`, `legend-engine-test-server-shared` |

Each of the 48 also pins `maven-dependency-plugin` to 3.1.2 — version 2.10 bundles an ASM that
throws on major-55 class files and would fail all 48.

---

## 5. What had to be decoupled

Two modules had dependencies pointing the wrong way — a lower layer reaching up into the HTTP
layer. Both were fixed by extracting the non-REST code into a new module. **Neither fix is specific
to Java 11; both are corrections that stand on their own merit.**

### Decoupling 1 — `hostedService-protocol` → `analytics-lineage-http-api`

A **protocol** module depended on an **HTTP API** module. One file (`SingleLineage.java`) imported
five plain POJOs.

**Fix:** new Java 8 module `legend-engine-xt-analytics-lineage-protocol` holding the whole
`org.finos.legend.engine.api.analytics.model.**` tree. Only the JAX-RS resource
(`LineageAnalytics.java`) stayed behind in `-http-api`.

**Payoff:** cleared four modules at once — `hostedService-protocol`, `-compiler`, `-generation` and
`-grammar`.

### Decoupling 2 — `extensions-collection-generation` → `graphQL-http-api`

The explicitly client-facing generation aggregate depended directly on a REST module. That module
held 30 classes, of which **only three were actually JAX-RS resources**. The other 27 — the
`GenerationExtension` / `ExternalFormatExtension` SPI implementations that `collection-generation`
genuinely needs, plus caches, directives and model/error POJOs — were ordinary library code sitting
in the wrong place.

**Fix:** new Java 8 module `legend-engine-xt-graphQL-api-core` holding those 27 classes and their
three `META-INF/services/` registrations. `collection-generation` and
`graphQL-relational-extension` now depend on `-api-core`.

One consequence found mid-split and worth noting as a process point: `TotalCountDirective` was
calling a static method *through a REST resource* (`GraphQLExecute.toPureModel`). It was relocated
to `GraphQLExecutionHelper`. An initial grep missed this because the output was truncated — **the
compiler caught it, the search did not.** We now treat compile-verification, not grep, as the
evidence that a decoupling is complete.

---

## 6. How the boundary is enforced (the part that makes this durable)

The enforcer ceiling was parameterised rather than hardcoded:

```xml
<maxJdkVersion>${maven.compiler.target}</maxJdkVersion>   <!-- was: 1.8 -->
```

**Why this matters:** the rule now tracks each module's own compile target. A module that moves to
Java 11 raises only *its own* ceiling; every module still on 1.8 keeps rejecting Java 11
dependencies automatically. The boundary maintains itself — there is no second list to keep in sync,
and a Java 8 module can never silently acquire a Java 11 dependency again.

Test-scoped dependencies are excluded (`<ignoredScopes>test</ignoredScopes>`): test bytecode is never
published to clients, and both CI and the supported dev JDKs are 11+.

**We proved the rule actually bites rather than silently defaulting.** Forcing
`-Dmaven.compiler.target=1.7` on `legend-engine-shared-core` makes the build fail with a list of
banned dependencies. A rule that never fails is not a rule.

---

## 7. Evidence

| Check | Result |
|---|---|
| Whole-reactor `mvn validate` | Passes across all 605 modules |
| Boundary scan (Java 8 module reaching a Java 11 one) | **0 violations** |
| Bytecode audit of emitted `target/classes` | Java 11 modules at major 55, all others at major 52, **0 mismatches** |
| Falsification test (force target 1.7) | Enforcer fails as expected — rule is live |
| graphQL suite after the split | Green (71 + 5 tests) |
| Patch applies to clean `45c0c423c1c` | `git am` clean; resulting tree byte-identical to the verified commit |

---

## 8. Two honest caveats

**The boundary later grew from 48 to 50.** When the Dropwizard upgrade landed (separate commit),
pac4j 5 turned out to be Java 11 bytecode, which forced two more modules across the line:
`legend-engine-xt-identity-pac4j` and `legend-engine-application-query`. We verified this did not
breach the client contract — all 35 consumers of `identity-pac4j` are themselves Java 11 REST
modules, and both client aggregates still reach zero Java 11 modules. **This is not part of this
patch**, but it shows the boundary is a live constraint that each dependency change must be
re-checked against, not a one-time exercise.

**A documentation figure was wrong, and has since been corrected.** The upgrade plan and ADR-003
both stated 31 inert `source`/`target` overrides; the measured number is **28**. Re-verified against
the base commit — 28 poms declared both `<maven.compiler.source>11</maven.compiler.source>` and
`<maven.compiler.target>11</maven.compiler.target>` at `45c0c423c1c`. Both documents now say 28.

---

## 9. What this patch does and does not deliver

**Delivers:** an enforced, self-documenting Java 8 / Java 11 line; two long-standing wrong-direction
dependency edges removed; 28 poms that now tell the truth about what they emit; a build that fails
loudly if anyone crosses the line.

**Does not deliver:** any vulnerability remediation. **No CVE is closed by this patch.** It is the
prerequisite that makes the Dropwizard 3.0 / Jetty 10 / Jackson 2.19 upgrade possible without
breaking Java 8 clients. That upgrade is the next commit and is still in progress.

**Known open question on the upgrade that follows:** two of the five findings name
`jetty-security` / `jetty-client` at **12.0.36**, while the planned upgrade delivers Jetty **10.0.26**.
Jetty 12 would require Dropwizard 5, the `jakarta.*` namespace migration and **Java 17** — which
contradicts the Java 11 premise of this entire effort. This needs confirmation from the scanner
owner before the upgrade work is considered complete, because it determines whether those two
findings close at all.

---

## 10. Cost and risk

- **Reversible.** A single commit; revert restores the prior state exactly.
- **Low behavioural risk.** 25 of the pom edits are provably no-ops on emitted bytecode (the
  declarations were already being ignored). The two decouplings are file moves plus one static-method
  relocation, all compile-verified and covered by the existing graphQL suite.
- **Ongoing cost.** 48 poms now carry compiler properties the root would otherwise supply, and two
  modules exist purely to break dependency cycles. Both are the visible price of making the
  client/server split explicit rather than conventional.
