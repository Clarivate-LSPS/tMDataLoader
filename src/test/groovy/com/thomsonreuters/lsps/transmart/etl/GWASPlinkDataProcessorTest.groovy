package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import spock.lang.Specification

/**
 * Date: 21-Apr-16
 * Time: 15:35
 */
class GWASPlinkDataProcessorTest extends Specification implements ConfigAwareTestCase {
    void setup() {
        ConfigAwareTestCase.super.setUp()
    }

    def "it should upload GWAS Plink data"() {
        setup:
        def processor = new GWASPlinkDataProcessor(config)
        def dataDir = Fixtures.studiesDir.studyDir('Test Study With GWAS Plink', 'GSE0GWASPLINK').toPath().
                resolve('GWASPlinkDataToUpload')


        when:
        def successfullyUploaded = processor.process(dataDir, [:])

        then:
        successfullyUploaded
    }
}
