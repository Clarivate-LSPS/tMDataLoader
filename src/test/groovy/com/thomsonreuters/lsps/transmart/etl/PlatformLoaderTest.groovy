package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.etl.platforms.GexPlatform

import static org.hamcrest.CoreMatchers.equalTo
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 3/31/14.
 */
class PlatformLoaderTest extends GroovyTestCase implements ConfigAwareTestCase {
    void testDoLoad() {
        sql.execute("delete from deapp.de_gpl_info where platform = ?", 'TST')
        sql.execute("delete from annotation_deapp where gpl_id = ?", 'TST')
        def platformLoader = new PlatformLoader(sql, config)
        def gexPlatform = new GexPlatform(new File('fixtures/Platforms/TST.txt'), 'TST', config)
        platformLoader.doLoad(gexPlatform, [:])
        def platformInfo = sql.firstRow("select * from deapp.de_gpl_info where platform = ?", 'TST')
        assertThat(platformInfo.organism, equalTo('Homo Sapiens'))
        assertThat(platformInfo.title, equalTo('Test Platform'))
        def cntRow = sql.firstRow("select count(*) from lt_src_deapp_annot where gpl_id = ?", 'TST')
        def emptyGeneId = sql.firstRow("select gene_id from lt_src_deapp_annot where gpl_id = ? and gene_symbol = ?", 'TST', 'ARX')
        assertNull(emptyGeneId['gene_id'])
        assertThat(cntRow[0] as long, equalTo(5L))
    }

    void testMultipleGeneIds() {
        sql.execute("delete from deapp.de_gpl_info where platform = ?", 'TST')
        sql.execute("delete from annotation_deapp where gpl_id = ?", 'TST')
        def conf = config.clone()
        conf.useFirstGeneId = true
        def platformLoader = new PlatformLoader(sql, conf)
        def gexPlatform = new GexPlatform(new File('fixtures/Platforms/TST.txt'), 'TST', conf)
        platformLoader.doLoad(gexPlatform, [:])
        def cntRow = sql.firstRow("select count(*) from lt_src_deapp_annot where gpl_id = ?", 'TST')
        def emptyGeneId = sql.firstRow("select gene_id from lt_src_deapp_annot where gpl_id = ? and gene_symbol = ?", 'TST', 'ARX')
        def multGeneId = sql.firstRow("select gene_id from lt_src_deapp_annot where gpl_id = ? and gene_symbol = ?", 'TST', 'MULTID')
        assertNull(emptyGeneId['gene_id'])
        assertThat(multGeneId['gene_id'] as long, equalTo(123L))
        assertThat(cntRow[0] as long, equalTo(6L))
    }
}
