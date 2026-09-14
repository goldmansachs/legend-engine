//  Copyright 2022 Goldman Sachs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package org.finos.legend.engine.language.pure.grammar.api.relationalOperationElement;

import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.finos.legend.engine.language.pure.grammar.from.RelationalGrammarParserExtension;
import org.finos.legend.engine.language.pure.grammar.from.extension.PureGrammarParserExtensions;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.operation.RelationalOperationElement;
import org.finos.legend.engine.shared.core.api.grammar.GrammarAPI;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.finos.legend.engine.shared.core.kerberos.ProfileManagerHelper;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.jax.rs.annotations.Pac4JProfileManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

import static org.finos.legend.engine.shared.core.operational.http.InflateInterceptor.APPLICATION_ZLIB;

@Tag(name = "Pure - Grammar")
@Path("pure/v1/grammar/grammarToJson")
public class RelationalOperationElementGrammarToJson extends GrammarAPI
{
    @POST
    @Path("relationalOperationElement")
    @Operation(summary = "Generates Pure protocol JSON from Pure language text for relational operation elements")
    @Consumes({MediaType.TEXT_PLAIN, APPLICATION_ZLIB})
    @Produces(MediaType.APPLICATION_JSON)
    public Response relationalOperationElement(String text,
                                               @DefaultValue("") @Parameter(description = "The source ID to be used by the parser") @QueryParam("sourceId") String sourceId,
                                               @DefaultValue("0") @Parameter(description = "The line number the parser will offset by") @QueryParam("lineOffset") int lineOffset,
                                               @DefaultValue("0") @Parameter(description = "The column number the parser will offset by") @QueryParam("columnOffset") int columnOffset,
                                               @DefaultValue("true") @QueryParam("returnSourceInformation") boolean returnSourceInformation,
                                               @Parameter(hidden = true) @Pac4JProfileManager ProfileManager pm)
    {
        PureGrammarParserExtensions.logExtensionList();
        return grammarToJson(text,
                (a) -> RelationalGrammarParserExtension.parseRelationalOperationElement(a, sourceId, lineOffset, columnOffset, returnSourceInformation), Identity.makeIdentity(ProfileManagerHelper.extractProfiles(pm)), "Grammar to Json : RelationalOperationElement");
    }

    // Required so that Jackson properly includes _type for the top level element
    static class TypedMap extends UnifiedMap<String, RelationalOperationElement>
    {
    }

    @POST
    @Path("relationalOperationElement/batch")
    @Operation(summary = "Generates Pure protocol JSON from Pure language text for relational operation elements")
    @Consumes({MediaType.APPLICATION_JSON, APPLICATION_ZLIB})
    @Produces(MediaType.APPLICATION_JSON)
    public Response relationalOperationElementBatch(Map<String, ParserInput> input, @Parameter(hidden = true) @Pac4JProfileManager ProfileManager pm)
    {
        PureGrammarParserExtensions.logExtensionList();
        return grammarToJsonBatch(input,
                RelationalGrammarParserExtension::parseRelationalOperationElement, new TypedMap(), Identity.makeIdentity(ProfileManagerHelper.extractProfiles(pm)), "Grammar to Json : RelationalOperationElement Batch");
    }
}
