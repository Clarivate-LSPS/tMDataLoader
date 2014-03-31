package com.thomsonreuters.lsps.transmart.etl

import static org.hamcrest.CoreMatchers.equalTo
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 3/31/14.
 */
class PlatformLoaderTest extends ConfigAwareTestCase {
    void testDoLoad() {
        sql.execute("delete from deapp.de_gpl_info where platform = ?", 'TST')
        sql.execute("delete from ${config.controlSchema}.annotation_deapp where gpl_id = ?", 'TST')
        def platformLoader = new PlatformLoader(sql, config)
        platformLoader.doLoad(new File('fixtures/Platforms/TST.txt'), 'TST', [:])
        def platformInfo = sql.firstRow("select * from deapp.de_gpl_info where platform = ?", 'TST')
        assertThat(platformInfo.organism, equalTo('Homo Sapiens'))
        assertThat(platformInfo.title, equalTo('Test Platform'))
        def cntRow = sql.firstRow('select count(*) from tm_lz.lt_src_deapp_annot where gpl_id = ?', 'TST')
        assertThat(cntRow[0] as long, equalTo(4L))
    }
}
