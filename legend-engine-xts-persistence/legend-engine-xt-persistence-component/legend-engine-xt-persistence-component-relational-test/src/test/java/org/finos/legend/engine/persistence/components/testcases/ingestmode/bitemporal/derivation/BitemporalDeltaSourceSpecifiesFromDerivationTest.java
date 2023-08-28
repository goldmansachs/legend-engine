// Copyright 2023 Goldman Sachs
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

package org.finos.legend.engine.persistence.components.testcases.ingestmode.bitemporal.derivation;

import org.finos.legend.engine.persistence.components.scenarios.BitemporalDeltaSourceSpecifiesFromOnlyScenarios;
import org.finos.legend.engine.persistence.components.scenarios.TestScenario;
import org.junit.jupiter.api.Test;

import static org.finos.legend.engine.persistence.components.BaseTest.assertDerivedMainDataset;

public class BitemporalDeltaSourceSpecifiesFromDerivationTest
{
    BitemporalDeltaSourceSpecifiesFromOnlyScenarios scenarios = new BitemporalDeltaSourceSpecifiesFromOnlyScenarios();

    @Test
    void testBitemporalDeltaBatchIdBasedNoDeleteIndNoDataSplits()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__NO_DEL_IND__NO_DATA_SPLITS();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdBasedNoDeleteIndWithDataSplits()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__NO_DEL_IND__WITH_DATA_SPLITS();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdBasedWithDeleteIndNoDataSplits()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__WITH_DEL_IND__NO_DATA_SPLITS();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdBasedWithDeleteIndWithDataSplitsUsingDefaultTempTable()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__WITH_DEL_IND__WITH_DATA_SPLITS__USING_DEFAULT_TEMP_TABLE();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdBasedNoDeleteIndNoDataSplitsFilterDuplicates()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__NO_DEL_IND__NO_DATA_SPLITS__FILTER_DUPLICATES();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdBasedNoDeleteIndWithDataSplitsFilterDuplicates()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__NO_DEL_IND__WITH_DATA_SPLITS__FILTER_DUPLICATES();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdBasedWithDeleteIndNoDataSplitsFilterDuplicates()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__WITH_DEL_IND__NO_DATA_SPLITS__FILTER_DUPLICATES();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdBasedWithDeleteIndWithDataSplitsFilterDuplicates()
    {
        TestScenario scenario = scenarios. BATCH_ID_BASED__WITH_DEL_IND__WITH_DATA_SPLITS__FILTER_DUPLICATES();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaBatchIdDateTimeBasedNoDeleteIndNoDataSplits()
    {
        TestScenario scenario = scenarios. BATCH_ID_AND_TIME_BASED__NO_DEL_IND__NO_DATA_SPLITS();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaDateTimeBasedNoDeleteIndNoDataSplits()
    {
        TestScenario scenario = scenarios.DATETIME_BASED__NO_DEL_IND__NO_DATA_SPLITS();
        assertDerivedMainDataset(scenario);
    }

    @Test
    void testBitemporalDeltaWithValidityFieldsHavingSameName()
    {
        TestScenario scenario = scenarios.BATCH_ID_BASED__VALIDITY_FIELDS_SAME_NAME();
        assertDerivedMainDataset(scenario);
    }
}
