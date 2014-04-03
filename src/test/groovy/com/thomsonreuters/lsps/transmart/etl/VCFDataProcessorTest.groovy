package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 4/3/14.
 */
class VCFDataProcessorTest extends ConfigAwareTestCase {
    String studyName = 'Test Study'
    String studyId = 'GSE0'

    void testItLoadsVCFFile() {
        //assertThat(db, hasSample())
    }
}
