package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static org.junit.Assert.assertThat

/**
 * Date: 21-Apr-16
 * Time: 15:35
 */
class GWASPlinkDataProcessorTest extends Specification implements ConfigAwareTestCase {
    final String studyId = 'GSE0GWASPLINK'
    final String studyName = 'Test Study With GWAS Plink'

    void setup() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_PROCESS_GWAS_PLINK_DATA.sql')
        Study.deleteById(config, studyId)
    }

    def "it should upload GWAS Plink data"() {
        setup:
        def gwasPlinkData = Fixtures.studiesDir.studyDir(studyName, studyId).getGWASPlinkData()

        when:
        def successfullyUploaded = gwasPlinkData.load(config)

        then:
        successfullyUploaded
        assertThat(sql, hasRecord('gwas_plink.plink_data', [study_id: gwasPlinkData.studyId], [:]))
        assertThat(sql, hasNode($/\Test Studies\$gwasPlinkData.studyName\GWAS\GWAS Plink\/$).withPatientCount(6))
    }
}
