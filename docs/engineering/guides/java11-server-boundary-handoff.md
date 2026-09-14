# Java 11 Server Boundary — Handoff

**Written:** 2026-09-11
**For:** whoever (human or agent) picks this up next, on the new 64 GB VM
**Related:** [Upgrade Plan](java11-server-boundary-plan.md) · [ADR-003](../decisions/ADR-003-java-11-server-boundary.md)

Read the plan for *what* and the ADR for *why each decision was made*. This file is only
**where things actually stand and what to do next.**

---

## 1. What exists right now

Two repositories, one branch each, both named `java11-server-boundary`.

### `finos-legend-engine`

Branch is rebased onto `gitlab/finos-master` at `45c0c423c1c` — **the rebase was clean, zero
conflicts**. Only four files overlapped with the twelve upstream commits and all merged cleanly.

| Commit | Contents | State |
|---|---|---|
| `453dae846f6` | Enforce a Java 8 / Java 11 compile boundary for server modules | **Verified green** |
| `04bb0e7b22c` | Upgrade Dropwizard 3.0.17 / Jetty 10.0.26 / Jackson 2.19.4 / pac4j 5.7.1 | **Incomplete — does not build** |

### `finos-legend-shared`

Branch off `origin/finos-master` at `a5a14d3` (already current, nothing to rebase).

| Commit | Contents | State |
|---|---|---|
| `fd4b5ff` | Upgrade Dropwizard 3.0.17 / Jetty 10.0.26 / pac4j 5.7.1 / Jackson 2.19.4 | **Verified green — all 83 tests pass** |

### Before you push

Both repos were committed with a placeholder identity (`Claude Code <claude-code@localhost>`)
because no `user.name`/`user.email` was configured on the old VM. Re-author before pushing:

```bash
git -c user.name="Your Name" -c user.email="you@gs.com" rebase --exec 'git commit --amend --no-edit --reset-author' -i gitlab/finos-master
```

---

## 2. What is proven, and what is not

**Commit 1 (boundary) is genuinely verified.**
- `mvn validate` passes across all 604 modules.
- Bytecode audit: 47/48 Java 11 modules at major 55 (the 48th is test-only, its `test-classes`
  are 55), 418 Java 8 modules at major 52, **zero mismatches**.
- Falsification test: forcing `-Dmaven.compiler.target=1.7` makes `enforce-bytecode-version` fail,
  so the parameterisation is genuinely read rather than silently defaulting.
- graphQL suite green (71 + 5 tests) after the two module splits.

This commit is a viable standalone PR.

**Commit 2 (upgrades) is not.** It compiles a long way through the reactor and then stops — see the
next section. Jackson 2.19.4 on its own was verified earlier (927 tests green across
`legend-engine-shared-core`, `legend-engine-protocol-pure`, `legend-engine-language-pure-grammar`,
`legend-engine-language-pure-compiler`) but that was before pac4j 5 landed, so re-verify.

---

## 3. Where the build stops — pick this up first

Full reactor build reaches roughly **480 of 604 modules** and fails here:

```
Failed to execute goal maven-enforcer-plugin:enforce (enforce-bytecode-version)
  on project legend-engine-xt-serviceStore-executionPlan:
  Found Banned Dependency: org.eclipse.jetty:jetty-servlet:jar:10.0.26
  Found Banned Dependency: org.eclipse.jetty:jetty-util:jar:10.0.26
```

**Diagnosis (already done — do not re-derive it).** `legend-engine-xt-serviceStore-executionPlan`
is a Java 8 module that declares `com.github.tomakehurst:wiremock-jre8` at **compile** scope, and
genuinely uses it from `src/main` (5 files). Wiremock bundles Jetty. It asks for Jetty 9.4, but the
root `dependencyManagement` now pins `org.eclipse.jetty:*` to 10.0.26, so the managed version wins
and wiremock drags Java 11 Jetty onto a Java 8 module's compile classpath. Under the old
Jetty 9.4.44 pin this was invisible.

Note this module is the *only* one declaring wiremock at compile scope — every other module has it
at `test`.

Candidate fixes, roughly in order of attractiveness:

1. **Switch that module to `wiremock-jre8-standalone`**, which shades its Jetty, so no transitive
   Jetty reaches the classpath at all. Smallest change; verify the shaded package names the code
   uses still resolve.
2. **Move the five `src/main` wiremock-using classes into a test-support module** that is Java 11
   (or test-scoped everywhere it is consumed). Architecturally the cleanest — a production module
   depending on a mocking library is the actual smell — but a larger change.
3. **Exclude `org.eclipse.jetty:*` from wiremock in that module** and let it resolve its own 9.4.
   Fights the dependencyManagement pin and will be fragile.

Do **not** just add Java 11 properties to that module without checking first: run the boundary
script in §6 to see whether it sits under `collection-execution`. If it does, making it Java 11
breaks the Java 8 client contract, which is the whole point of this work.

---

## 4. After that, expect more of the same class of problem

Every remaining failure so far has been one of four recurring shapes. Recognising them saves time:

- **A Java 8 module acquiring a Java 11 transitive.** Same as the wiremock case. Either the
  dependency is genuinely server-side (make the module Java 11, after checking it is not under a
  client aggregate) or it has leaked (exclude / re-scope it).
- **Baseline-installed poms in `~/.m2` referencing coordinates that were swapped.** Symptom:
  `The POM for X is invalid, transitive dependencies will not be available`. Cause: a module pom
  installed before the migration still names `javax.ws.rs:javax.ws.rs-api` etc., which no longer has
  a managed version. A full reactor build fixes it; piecemeal `-pl` builds will keep tripping on it.
- **Duplicate providers of the same packages.** `javax.servlet.*` is supplied by both
  `jakarta.servlet-api` and Jetty 10's `jetty-servlet-api`; `javax.ws.rs.*` by both
  `jakarta.ws.rs-api` and `javax.ws.rs-api`. The root pom already excludes `jetty-servlet-api` from
  all 11 managed Jetty artifacts. Expect more of these.
- **`dependencyConvergence` failures.** Add a managed pin next to the existing
  `<!-- Needed to resolve dependency convergence issue -->` entries. Already pinned:
  `jakarta.inject-api`, `caffeine`, `classmate`, `jackson-module-jaxb-annotations`, plus an
  imported `dropwizard-bom` (imported **last** so the explicit jackson/jetty/jersey pins still win).

## 5. Then the things that have never been exercised

None of the following has been run even once. Treat all of it as unknown, not as "probably fine":

1. **Full reactor build with tests.** Only `-DskipTests` has been attempted on engine.
2. **Server startup.**
   `server legend-engine-config/legend-engine-server/legend-engine-server-http-server/src/test/resources/org/finos/legend/engine/server/test/userTestConfig.json`
   then round-trip `POST /api/pure/v1/grammar/transformGrammarToJson`, `/compilation/compile`,
   `/executionPlan/generate`, `/execution/execute`.
3. **An authenticated request.** This is the highest-risk untested area: the pac4j 5 port rewrote
   `LegendSecurityLogic`, four `SessionStore` implementations and the whole client/authenticator
   surface, and unit tests do not cover bundle wiring. Exercise a real login path.
4. **Swagger.** Annotations moved from Swagger 1.5 to OpenAPI 3 across 82 files, so the published
   spec shape changes. Confirm `http://127.0.0.1:6300/api/swagger` still renders and check whether
   anything downstream consumed the old Swagger 1.2 JSON.
5. **Pure IDE.** `PureIDELight` with `ideLightConfig.json` — read the port from the config, do not
   assume 9010.
6. **`mvn checkstyle:check`** in both repos; new files need the Apache 2.0 header.

---

## 6. Useful scripts

**Verify the boundary holds** (zero violations expected; prints any Java 8 module that can reach a
Java 11 one):

```bash
python3 - <<'PY'
import os,xml.etree.ElementTree as ET
NS='{http://maven.apache.org/POM/4.0.0}'
poms={}
for root,dirs,files in os.walk('.'):
    if 'target' in root.split(os.sep) or '.git' in root.split(os.sep): continue
    if 'pom.xml' in files:
        p=os.path.join(root,'pom.xml')
        try: r=ET.parse(p).getroot()
        except Exception: continue
        poms[r.findtext(NS+'artifactId')]=(p,r)
deps={}
for a,(p,r) in poms.items():
    d=set()
    for c in r:
        if c.tag!=NS+'dependencies': continue
        for dep in c:
            n=dep.findtext(NS+'artifactId') or ''; sc=dep.findtext(NS+'scope') or 'compile'
            if n in poms and sc in ('compile','runtime','provided'): d.add(n)
    deps[a]=d
memo={}
def clos(a,st=()):
    if a in memo: return memo[a]
    if a in st: return set()
    r=set()
    for x in deps.get(a,()): r.add(x); r|=clos(x,st+(a,))
    memo[a]=r; return r
J11=set()
for a,(p,r) in poms.items():
    pr=r.find(NS+'properties')
    if pr is not None and pr.findtext(NS+'maven.compiler.release')=='11': J11.add(a)
viol=[(a,sorted(J11&clos(a))) for a in poms if a not in J11 and a!='legend-engine' and (J11&clos(a))]
print(f"java11 modules={len(J11)}  violations={len(viol)}")
for a,b in viol: print("  ",a,"->",b)
PY
```

**Audit emitted bytecode** (expects Java 11 modules at major 55, everything else at 52):

```bash
python3 - <<'PY'
import os,struct,xml.etree.ElementTree as ET
NS='{http://maven.apache.org/POM/4.0.0}'
bad=ok=0
for root,dirs,files in os.walk('.'):
    if 'pom.xml' not in files: continue
    try: r=ET.parse(os.path.join(root,'pom.xml')).getroot()
    except Exception: continue
    pr=r.find(NS+'properties')
    exp=55 if (pr is not None and pr.findtext(NS+'maven.compiler.release')=='11') else 52
    cd=os.path.join(root,'target','classes'); mj=None
    for rr,_,ff in os.walk(cd):
        for f in ff:
            if f.endswith('.class'):
                mj=struct.unpack('>H',open(os.path.join(rr,f),'rb').read(8)[6:8])[0]; break
        if mj: break
    if mj is None: continue
    if mj==exp: ok+=1
    else: bad+=1; print("MISMATCH",root,"major",mj,"expected",exp)
print(f"ok={ok} mismatches={bad}")
PY
```

**Check a jar's real bytecode level before trusting a version bump** — this is how the mis-targeted
Jackson artifacts were caught:

```bash
python3 -c "
import zipfile,struct,sys
z=zipfile.ZipFile(sys.argv[1]); mx=0;w=''
for n in z.namelist():
    if n.endswith('.class') and 'module-info' not in n and not n.startswith('META-INF/versions/'):
        m=struct.unpack('>H',z.read(n)[6:8])[0]
        if m>mx: mx,w=m,n
print('major',mx,w)" some.jar
```

---

## 7. Build discipline on the new VM

The old VM had 32 GB and **crashed** running a full reactor build alongside other work. With 64 GB
and `-Xmx48g` you have more headroom, and the user has confirmed `-T 4` is fine. Still:

- **Build legend-shared first and install it**, then engine. Engine's `legend.shared.version` is
  `0.37.1-SNAPSHOT`, which exists only locally until legend-shared is published.
  ```bash
  cd finos-legend-shared && mvn clean install -T 4
  cd ../finos-legend-engine && mvn clean install -DskipTests -T 4 \
      -Dnpm.registry.url=https://npm.aws.site.gs.com/repository/npm-group
  ```
- `legend-engine-pure-ide-light-http-server` needs that `-Dnpm.registry.url` flag or it tries to
  reach `registry.npmjs.org` and fails.
- **Do not pass `-Drevision`.** `CLAUDE.md` recommends it for cache isolation, but the installed
  baseline is at the default `4.145.1-SNAPSHOT`; overriding it makes single-module builds fail to
  resolve their already-installed siblings and forces a multi-hour `-am` rebuild.
- After editing the root `pom.xml`, run `mvn -N -DskipTests install` so modules resolving the parent
  from the repository see the new `dependencyManagement`.
- Always pass `clean` — several Pure Maven plugins fail on stale `target/` directories.

---

## 8. Open questions worth raising rather than deciding alone

- **Jetty 10 is end-of-life.** 10.0.26 and 11.0.26 are the final releases of those lines. Two of the
  five findings actually name `jetty-security` / `jetty-client` **12.0.36**, which means Dropwizard 5,
  the `jakarta.*` namespace and Java 17. The user accepted Jetty 10 for this round; confirm the
  scanner does too, and expect to revisit.
- **The published API spec changes shape** (Swagger 1.2 → OpenAPI 3). Worth confirming with whoever
  consumes `/api/swagger` before this merges.
- **`legend-shared` must be released** before engine's commit 2 can merge anywhere but a branch.
- **`maven-dependency-plugin` is pinned to 3.1.2 per-module**, not globally. 3.3.0+ adds a stricter
  analyzer that flags pre-existing pom hygiene in about 30 modules — a worthwhile but separate
  cleanup, deliberately not bundled here.
