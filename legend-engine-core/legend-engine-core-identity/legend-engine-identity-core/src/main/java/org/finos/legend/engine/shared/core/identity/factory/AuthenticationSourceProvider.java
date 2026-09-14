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

package org.finos.legend.engine.shared.core.identity.factory;

import org.finos.legend.engine.shared.core.identity.Identity;

/**
 * Supplies the authentication argument that some Pure-generated graph-fetch methods declare.
 *
 * <p>The only implementation is pac4j-based and lives in {@code legend-engine-xt-identity-pac4j}.
 * It is reached through {@link java.util.ServiceLoader} rather than a direct dependency so that the
 * Java 8 execution core does not compile against pac4j, which ships Java 11 bytecode from 5.x
 * onwards. See ADR-003.
 */
public interface AuthenticationSourceProvider
{
    /**
     * The declared parameter type on the generated method, used for reflective lookup.
     */
    Class<?> parameterType();

    /**
     * The value to pass for that parameter.
     */
    Object valueFor(Identity identity);

    /**
     * Name of the authentication client that produced this identity, or null if it cannot be determined.
     */
    String clientName(Identity identity);
}
