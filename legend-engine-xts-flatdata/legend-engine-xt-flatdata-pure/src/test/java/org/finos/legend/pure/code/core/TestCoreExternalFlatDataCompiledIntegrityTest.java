package org.finos.legend.pure.code.core;

import org.finos.legend.pure.m3.tests.AbstractCompiledStateIntegrityTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestCoreExternalFlatDataCompiledIntegrityTest extends AbstractCompiledStateIntegrityTest
{
    @BeforeClass
    public static void initialize()
    {
        initialize("core_external_format_flatdata");
    }

    @Test
    @Ignore
    @Override
    public void testReferenceUsages()
    {
        // TODO fix this test
        super.testReferenceUsages();
    }
}