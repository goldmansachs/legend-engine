// Copyright 2022 Goldman Sachs
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

package org.finos.legend.engine.api.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.protocol.pure.v1.PureProtocolObjectMapperFactory;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.PackageableElement;
import org.finos.legend.pure.generated.Root_meta_analytics_quality_model_ViolationInstance;
import org.finos.legend.pure.generated.core_analytics_quality_checksEngine;
import org.finos.legend.pure.runtime.java.compiled.execution.CompiledExecutionSupport;
import org.pac4j.core.profile.CommonProfile;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DataspaceQualityAnalyticsHelper
{
    public static ObjectMapper getNewObjectMapper()
    {
        return PureProtocolObjectMapperFactory.getNewObjectMapper();
    }

    public static String validDataspaceResponseString = "Dataspace is valid.";


    public static Response getResponse(MutableList<CommonProfile> profiles, PureModel pureModel, List<PackageableElement> allElements)
    {
        CompiledExecutionSupport es = pureModel.getExecutionSupport();

        FastList<org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement> _elements = FastList.newList();
        allElements.forEach(elem -> _elements.add(pureModel.getContext().resolvePackageableElement(elem.getPath(), elem.sourceInformation)));
        RichIterable<? extends Root_meta_analytics_quality_model_ViolationInstance<?>> result = core_analytics_quality_checksEngine.Root_meta_analytics_quality_model_domain_runQualityChecks_PackageableElement_MANY__ViolationInstance_MANY_(_elements, es);
        if (result.isEmpty())
        {
            return Response.ok().build();
        }
        return Response.ok().entity(aggregateErrorMessages(result)).build();
    }

    private static List<Map<String, String>> aggregateErrorMessages(RichIterable<? extends Root_meta_analytics_quality_model_ViolationInstance<?>> result)
    {
        List<Map<String, String>> totalErrors = new ArrayList<>();
        for (Root_meta_analytics_quality_model_ViolationInstance<?> violationInstance : result)
        {
            Map<String, String> errorMap = new HashMap<>();
            errorMap.put("packageableElementName", violationInstance._source().toString());
            errorMap.put("violationType", violationInstance._rule()._id());
            errorMap.put("errorMessage", violationInstance._detail()._message());
            errorMap.put("ruleDescription", violationInstance._rule()._description());
            totalErrors.add(errorMap);
        }
        return totalErrors;
    }

}
