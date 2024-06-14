package org.finos.legend.engine.protocol.pure.v1.model.test.result;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class TestDebug
{
    @JsonProperty(required = true)
    public String testable;

    public String testSuiteId;

    @JsonProperty(required = true)
    public String atomicTestId;


    public TestError error;
}
