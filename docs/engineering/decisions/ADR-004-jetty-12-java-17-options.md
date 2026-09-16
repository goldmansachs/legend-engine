# ADR-004: Jetty 12 / Java 17, or stay on Jetty 10 — options and trade-offs

**Status:** Proposed — decision not yet taken
**Date:** 2026-09-16
**Supersedes in part:** [ADR-003](ADR-003-java-11-server-boundary.md) D-15, which accepted Jetty 10
**Related:** [Upgrade plan](../guides/java11-server-boundary-plan.md)

---

## Context

ADR-003 D-15 accepted Jetty 10.0.26 on the reasoning that the five findings were internally
inconsistent — `jetty-http` was listed as fixed at ≥10.0.0 while `jetty-security` and `jetty-client`
named 12.0.36. That reasoning no longer holds:

- **CVE-2026-10050 genuinely requires Jetty 12.0.36**; 10.0.26 does not clear it.
- A second advisory affects **Jetty 12.1.0 through 12.1.10**, so the floor is **12.1.11 or later**.
  This also removes the 12.0.x line from consideration.

The effective requirement is therefore **Jetty ≥ 12.1.11**, and Jetty 12 is Java 17 bytecode
(major 61, verified on both the 12.0 and 12.1 lines). There is no Jetty 12 on Java 11.

This document records what was measured, and the trade-offs between upgrading and staying put.
Effort figures are judgement calls; every count is measured.

---

## What was verified

Measured on `java11-server-boundary`, not inferred.

| Claim | Evidence |
|---|---|
| **`jetty-client` is not in the dependency graph at all** | Absent from every `pom.xml`; **zero** occurrences across the engine-server, PureIDELight and Postgres-server runtime classpaths; `~/.m2` only ever fetched it at **9.4.49**, never 10.0.26 |
| It used to be there via wiremock | `wiremock-jre8 → jetty-proxy 9.4.x → jetty-client 9.4.x`. Commit `04bb0e7b22c` moved every consumer to `wiremock-jre8-standalone`, severing the edge. The scan named `jetty-client:9.4.57` — exactly that path |
| **`jetty-security` cannot be excluded** | `jetty-servlet:10.0.26` declares it **non-optional** (contrast `jetty-util-ajax` beside it, which is `<optional>true`); `ServletContextHandler` takes `SecurityHandler` in its constructors and resolves `ConstraintSecurityHandler` as a field initialiser. Also a direct compile dep of `dropwizard-core` and `dropwizard-jetty` |
| **`jetty-security` cannot be bumped in isolation** | `jetty-security:12.0.36` does not contain `ConstraintSecurityHandler`, `ConstraintMapping` or `ConstraintAware` — they moved to Jetty 12's ee9/ee10 modules. Swapping it in fails on first servlet context |
| **Our code never uses Jetty security** | Zero imports of `org.eclipse.jetty.security.*`; no `LoginService`, `ConstraintMapping`, `setSecurityHandler` or `setSecurityEnabled` anywhere. Authentication is entirely pac4j at the Jersey filter layer |
| **Jetty 12 requires Java 17** | `jetty-server` 12.0.36 and 12.1.1 are both max class-file major **61** |
| **No Dropwizard ships Jetty ≥12.1.11** | DW 5.0.0 → 12.1.1, 5.0.1 → 12.1.5, 5.0.2 → 12.1.9. 5.0.2 is the newest release. DW 4.x ships Jetty 11 |
| **DW 5.0.2 is binary-compatible with Jetty 12.1.13** | Extracted all 164 distinct Jetty method calls across 210 classes in 8 Dropwizard modules and resolved each by reflection under JDK 17: **162 resolve, 0 genuinely missing**. The 2 remaining are `jetty-setuid` `RLimit.setHard/setSoft(long)` vs the artifact's `(int)` — a DW-vs-setuid mismatch at *any* Jetty version, in an optional privilege-dropping path we do not use |
| **Jackson 2.21.4 keeps the Java 8 client contract** | All nine Jackson artifacts this repo uses are class-file **major 52** at 2.21.4 (and at 2.22.2) |

---

## Option A — Upgrade: Java 17 / Dropwizard 5.0.2 / Jetty 12.1.13

### Target stack

| Component | Version | Note |
|---|---|---|
| Java | **17** | Forced by Jetty 12 |
| Dropwizard | **5.0.2** | Newest release |
| Jetty | **12.1.13**, pinned up | DW ships 12.1.9; verified compatible. Watch for DW 5.0.3 shipping ≥12.1.11 natively and removing the pin |
| Jackson | **2.21.4** | Import `jackson-bom` — see the trap below |
| Jersey | 3.1.11 | DW native |

### Measured migration surface

| Item | Files | Notes |
|---|---:|---|
| `javax.ws.rs` imports | **150** | across **55 modules** — 45 already Java 11, **10 still Java 8** |
| `javax.servlet` imports | **55** | concentrated in the server modules |
| `javax.inject` imports | 4 | trivial |
| `javax.annotation` / `javax.validation` / `javax.xml.bind` in Java | **0** | none in source |
| Jetty imports (the Handler rewrite) | **8 files / 6 modules** | 5 in `src/main`; `Server`, `BaseServer`, `PureIDEServer`, `PureIDELight_NoExtension`, `PostgresServer` |
| Modules at `release=11` → 17 | **50** | |
| Modules at `release=8` | **556** | **do not move** — see below |

### The single most important scoping point

**Only the ~50 Java 11 modules need to reach Java 17, not all 606** — because the boundary from
ADR-003 already exists. That work is what makes this affordable at all. Anyone sizing this as
"move the reactor to 17" is sizing it roughly 10× too large.

### The five real risks

1. **`legend-engine-shared-core` collides with the Java 8 client contract.** It is reachable from
   **both** client aggregates (so must stay major 52) **and** uses `javax.ws.rs` in 5 files
   (`Response`, `MediaType`, `WebApplicationException`, `ReaderInterceptor`).
   `jakarta.ws.rs-api` 3.x requires Java 11+. Ten Java 8 modules use `javax.ws.rs` in total.
   This is a direct contradiction of the premise the whole effort protects.
   *Tractable* by the same extract-a-module pattern as D-7/D-8 — `ExceptionTool` is referenced by
   33 Java 11 modules but only 3 Java 8 ones — but it is real work on the most sensitive boundary
   in the repo, and it must be got right.

2. **The Pure generators emit `javax.*` into code compiled at runtime.** 20 `javax.` sites across
   `.pure` files. **5 must migrate** (4 × `javax.annotation.Generated`, 1 ×
   `javax.validation.constraints.NotNull`); **15 must NOT** (10 × `javax.xml.namespace.QName`,
   5 × `javax.xml.stream.*` — JDK-owned). No Java-side import rewrite touches these, no compile
   error catches a mistake, and the only signal is a golden-file diff or a runtime
   `ClassNotFoundException` inside a graph-fetch plan. A blind find/replace here breaks XML
   graph-fetch.

3. **`dropwizard-swagger` has no jakarta successor.** 4 POMs depend on it; 41 modules consume the
   annotations it scans. This is a removal and re-implementation of the OpenAPI wiring, not a bump.

4. **pac4j 6 is a breaking API migration.** `org.pac4j.jax-rs:core` reaches 36 POMs / 35 modules;
   pac4j 6 reshapes `WebContext`, `SessionStore` and profile management. `pac4j-javaee` →
   `pac4j-jakartaee` is an artifact *rename* a version-only sweep will miss. And **`legend-shared`
   must migrate first** — the engine cannot lead.

5. **A mechanical `javax.*` → `jakarta.*` rewrite is actively dangerous here.** 109 files import
   JDK-owned `javax.*` packages with no jakarta equivalent: `javax.security.auth` (53),
   `javax.xml.namespace` (23), `javax.tools` (12), `javax.sql` (11), `javax.crypto` (5),
   `javax.xml.stream` (3), `javax.net.ssl` (2). `javax.tools` is load-bearing — it is the runtime
   Java compiler used by plan execution. Any tooling must use an explicit six-package allowlist,
   never a prefix match.

### Smaller but real

- **Jackson trap:** from 2.20 onward `jackson-annotations` has a decoupled version line — it is
  published as `2.21`, **not** `2.21.4`. The root pom pins it to `${jackson.version}`, which will
  404 on the first build. Fix by importing `jackson-bom` (it carries `jackson.version.annotations`).
- **Jackson jakarta renames:** `jackson-jaxrs-json-provider` →
  `com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider` (**groupId changes**), and
  `jackson-module-jaxb-annotations` → `jackson-module-jakarta-xmlbind-annotations`. Both verified
  to exist and both Java 8.
- **Test tooling:** `byte-buddy` **1.11.20** and `mockito-core` **4.4.0** predate Java 17 support;
  `mockito-inline` is at 5.2.0, a mismatched major. `maven-compiler-plugin` is **3.8.0** (2018).
- **Jetty 12 rewrote `Handler`.** `Handler.Abstract`, and `Request`/`Response` are no longer servlet
  types. Only 8 files import Jetty, but ~4 of them need genuine redesign rather than a rename.
- **Java 17 encapsulation:** 16 files call `setAccessible` (each a potential
  `InaccessibleObjectException`); 2 files use `SecurityManager`/`doPrivileged` (deprecated in 17,
  Kerberos path).

### Effort — judgement, grounded in the counts above

| Workstream | Rough size |
|---|---|
| `legend-shared` migrated first (separate repo) | prerequisite, unknown until scoped |
| Split `legend-engine-shared-core` + the other 9 Java 8 `javax.ws.rs` modules | **high** — sensitive |
| `javax.*` → `jakarta.*` across 150 + 55 files / 55 modules | **medium**, mostly mechanical, flat tail |
| Pure generator sites (5 change, 15 must not) + golden files | **medium**, high-risk, hand review |
| pac4j 5 → 6 across 35 modules | **high** |
| Replace `dropwizard-swagger` | **medium–high**, no drop-in |
| Jetty 12 Handler rewrite (~4 files) | **medium**, low volume / high per-file |
| 50 modules to Java 17 + build-plugin bumps + test tooling | **low–medium** |
| Re-run the whole verification programme | **medium** |

**Not a continuation of the current branch — a separate programme of work.**

---

## Option B — Stay on Java 11 / Jetty 10.0.26 and remove the two artifacts

This is the cheap option if it works. It half-works.

### `jetty-client` — already done, at zero cost

It is **not in the dependency graph**. The wiremock change in `04bb0e7b22c` removed it. Nothing to
exclude, no upgrade required.

**Action:** close the finding as *artifact not present*, and add `jetty-client` to the enforcer's
banned-dependency list so it cannot silently return. Stronger and cheaper than an exclusion.

### `jetty-security` — not removable

Two independent, non-optional paths (`jetty-servlet` and `dropwizard-core`/`dropwizard-jetty`), and
`ServletContextHandler` resolves `ConstraintSecurityHandler` during construction. Excluding it
yields `NoClassDefFoundError` the moment any servlet context is built — which includes both the
Dropwizard server and the embedded Jetty in `PostgresServer`. Bumping it alone is impossible: the
12.0.36 artifact no longer contains the classes Jetty 10 needs.

The only ways to shed it are to stop using `ServletContextHandler` — i.e. abandon Dropwizard and the
servlet stack — or to move to Jetty 12. Neither is a workaround.

### What can honestly be said instead

**The class is present but the functionality is never activated.** No `LoginService`,
`ConstraintMapping`, `Authenticator`, `ConstraintSecurityHandler` or `setSecurityEnabled` anywhere
in the codebase; authentication is done entirely by pac4j at the Jersey filter layer. jetty-security
is on the classpath because `ServletContextHandler` needs the class to *resolve*, not because any
Jetty security handler is ever configured.

That is a compensating-control argument for a risk acceptance, **not** remediation. Whether it holds
depends on whether CVE-2026-10050 is reachable without a configured `SecurityHandler` — which
requires reading the advisory. It should be put to the scanner owner explicitly as a risk
acceptance, not presented as a fix.

### Other things considered and rejected

- **Shading/relocating Jetty** to hide it from the scanner: this conceals the artifact without
  removing the vulnerable code. Scanner avoidance, not remediation. Rejected.
- **Vendoring a patched `jetty-security` 10.0.26**: technically possible, but Jetty 10 is EOL, it
  means owning a security patch in-house indefinitely, and it needs the upstream fix to backport.
  Only worth considering if Option A is deferred for a long period and the risk acceptance is
  refused.

---

## Trade-offs

| | **A — Upgrade** | **B — Stay, remove what we can** |
|---|---|---|
| CVE-2026-10050 (`jetty-security`) | **Closed** | **Not closed** — risk acceptance required |
| `jetty-client` finding | Closed | **Closed** (already absent) |
| Effort | Large, multi-repo programme | Near zero |
| Java 8 client contract | **At risk** — needs the `shared-core` split to preserve | **Unaffected** |
| Behavioural risk | High: jakarta, pac4j 6, Jetty Handler rewrite, generated code | None |
| Dependency currency | Current and supportable | Jetty 10 is EOL; the next Jetty CVE has no answer |
| Blocks on another team | Yes — `legend-shared` must go first | No |
| Verification cost | Full programme re-run | Already done on this branch |

The asymmetry worth naming: **Option B is free but buys no time.** Jetty 10.0.26 is the final
release of a dead line. Every future Jetty advisory lands in the same position, with the same
answer unavailable. Option A is the only path that ends the recurrence.

---

## Recommendation

1. **Close `jetty-client` now** on the evidence. One of the two 12.0.36 findings, at zero cost.
2. **Put the `jetty-security` compensating control to the scanner owner** as an explicit,
   time-boxed risk acceptance — present but never activated.
3. **Ship the current branch.** It clears the `jackson-databind` and `jetty-http` findings and is
   verified green; holding it back gains nothing.
4. **Scope Option A as its own programme**, sequenced: `legend-shared` → `shared-core` split →
   server modules → Jetty 12 handlers. Frame it as *raising the existing boundary from 11 to 17*,
   which is what makes it affordable.
5. **Re-check before committing to A:** confirm with the scanner that Jetty **12.1.11+** actually
   clears both advisories. Everything in Option A depends on that being the right target.

---

## Open questions

- Is CVE-2026-10050 reachable without a configured `SecurityHandler`? Decides whether the
  compensating-control argument survives contact with the scanner.
- Does the second advisory affect the 12.0 line as well as 12.1.0–12.1.10? If 12.0.36 is clean, the
  DW 5.0.0 + 12.0.36 pin (also verified compatible) is an alternative target.
- When is Dropwizard 5.0.3 expected, and will it ship Jetty ≥12.1.11 natively?
- What is the `legend-shared` jakarta timeline? It gates everything in Option A.
