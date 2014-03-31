package com.thomsonreuters.lsps.transmart.files

import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.nullValue
import static org.hamcrest.MatcherAssert.assertThat

/**
 * Created by bondarev on 3/28/14.
 */
class GplFileTest extends GroovyTestCase {
    void testThatItReadsMetaInfo() {
        def gpl570 = new GplFile(new File("fixtures/Platforms/GPL570.txt"))
        def tst = new GplFile(new File("fixtures/Platforms/TST.txt"))
        assertThat(gpl570.metaInfo.PLATFORM_ID, nullValue())
        assertThat(gpl570.metaInfo.PLATFORM_TITLE, nullValue())
        assertThat(gpl570.metaInfo.PLATFORM_SPECIES, nullValue())

        assertThat(tst.metaInfo.PLATFORM_ID, equalTo('test_platform_1'))
        assertThat(tst.metaInfo.PLATFORM_TITLE, equalTo('Test Platform'))
        assertThat(tst.metaInfo.PLATFORM_SPECIES, equalTo('Homo Sapiens'))
    }
}
