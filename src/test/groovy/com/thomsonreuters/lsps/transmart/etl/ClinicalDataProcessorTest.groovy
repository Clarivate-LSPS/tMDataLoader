package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import groovy.sql.Sql
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.Fixtures.getAdditionalStudiesDir
import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class ClinicalDataProcessorTest extends Specification implements ConfigAwareTestCase {
    ClinicalData clinicalData = Fixtures.clinicalData
    private ClinicalDataProcessor _processor

    String studyName = 'Test Study'
    String studyId = 'GSE0'

    void setup() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_LOAD_CLINICAL_DATA.sql')
    }

    ClinicalDataProcessor getProcessor() {
        _processor ?: (_processor = new ClinicalDataProcessor(config))
    }

    void testItLoadsAge() {
        setup:
        clinicalData.load(config)

        expect:
        assertThat(db, hasRecord('i2b2demodata.patient_dimension',
                ['sourcesystem_cd': "${studyId}:HCC827"], [age_in_years_num: 20]))
    }

    def 'it should produce SummaryStatistic.txt'() {
        when:
        def expectedFile = new File(clinicalData.dir, 'ExpectedSummaryStatistic.txt')
        def actualFile = new File(clinicalData.dir, 'SummaryStatistic.txt')
        actualFile.delete()
        clinicalData.load(config)

        then:
        actualFile.exists()
        actualFile.text == expectedFile.text
    }

    def "it should collect statistic"() {
        setup:
        def processor = new ClinicalDataProcessor(config)
        database.withSql { sql ->
            processor.processFiles(clinicalData.dir, sql as Sql,
                    [name: clinicalData.studyName, node: "Test Studies\\${clinicalData.studyName}".toString()])
        }
        def statistic = processor.statistic

        expect:
        statistic != null
        statistic.tables.keySet() as List == ['TST001.txt', 'TST_DEMO.txt']
        def demo = statistic.tables.'TST_DEMO.txt'
        demo != null
        demo.variables.keySet() as List == ['SUBJ_ID', 'Age In Years', 'Sex', 'Assessment Date', 'Language']

        def subjId = demo.variables.SUBJ_ID
        subjId.notEmptyValuesCount == 9
        subjId.emptyValuesCount == 0
        subjId.required
        subjId.missingValueIds == []

        def age = demo.variables.'Age In Years'
        age.notEmptyValuesCount == 9
        age.emptyValuesCount == 0
        age.mean.round(6) == 30.555556
        age.median == 20.9
        age.min == 11.5
        age.max == 90.0
        age.standardDerivation.round(6) == 23.734843
        age.required
        age.missingValueIds == []
        age.violatedRangeChecks == [
                '30-50'                      : ['HCC2935', 'HCC4006', 'HCC827', 'NCIH3255', 'PC14', 'SKMEL28', 'SW48'],
                'Between 30 to 50'           : ['HCC2935', 'HCC4006', 'HCC827', 'NCIH3255', 'PC14', 'SKMEL28', 'SW48'],
                'Greater than 30, when "Sex" is equal to "Male"'            : ['HCC827', 'NCIH3255'],
                '>30'                        : ['HCC2935', 'HCC4006', 'HCC827', 'NCIH3255', 'PC14', 'SW48'],
                'Greater than or equal to 20': ['NCIH3255', 'SW48'],
                '>=20'                       : ['NCIH3255', 'SW48'],
                'Lesser than 50'             : ['SKMEL28'],
                '<50'                        : ['SKMEL28'],
                'Lesser than or equal to 20' : ['HCC4006', 'HCC827', 'NCIH1650', 'NCIH1975', 'PC14', 'SKMEL28'],
                '<=20'                       : ['HCC4006', 'HCC827', 'NCIH1650', 'NCIH1975', 'PC14', 'SKMEL28']
        ]

        def sex = demo.variables.'Sex'
        sex.notEmptyValuesCount == 7
        sex.emptyValuesCount == 2
        sex.factor.counts.Female == 5
        sex.factor.counts.Male == 2
        sex.required
        sex.missingValueIds == ['HCC4006', 'SW48']
        sex.violatedRangeChecks == [:]

        def assessmentDate = demo.variables.'Assessment Date'
        assessmentDate.notEmptyValuesCount == 9
        assessmentDate.emptyValuesCount == 0
        !assessmentDate.required
        assessmentDate.missingValueIds == null

        def language = demo.variables.'Language'
        language.notEmptyValuesCount == 3
        language.emptyValuesCount == 6
        !language.required
        language.missingValueIds == null
    }

    void testItLoadsData() {
        expect:
        String conceptPath = "\\Test Studies\\${studyName}\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"

        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient('HCC2935').inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M\\'))
    }

    void testItLoadsDataWithTags() {
        setup:
        ClinicalData tagClinicalData = Fixtures.studiesDir.studyDir('Test Study Tag', 'GSE0TAG').clinicalData
        String conceptPath = "\\Test Studies\\${tagClinicalData.studyName}\\"
        String conceptPathForPatient = conceptPath + tagClinicalData.studyId + '\\eText\\'

        tagClinicalData.load(config)

        expect:
        assertThat(sql, hasPatient('HCC2935').inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient + 'tag1\\').withPatientCount(5))
        assertThat(sql, hasNode(conceptPathForPatient + 'tag2\\').withPatientCount(4))
    }

    void testItMergesData() {
        expect:
        String conceptPath = "\\Test Studies\\${studyName}\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"
        String subjId = 'HCC2935'

        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M\\'))

        processor.process(
                new File(studyDir(studyName, studyId, additionalStudiesDir), "ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M TEST\\'))

        assertThat(sql, hasNode($/${conceptPath}Biomarker Data\Mutations\TST002 (Entrez ID: 324)\/$))
    }

    def 'it should load study with non-unique column names'() {
        when:
        def studyId = 'GSE0'
        def studyName = 'Test Study With Non Unique Column Names'
        config.allowNonUniqueColumnNames = true
        def processor = new ClinicalDataProcessor(config)
        processor.process(new File(studyDir(studyName, studyId, additionalStudiesDir), "ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        then:
        noExceptionThrown()
    }

    def 'it should load category_cd and data_label with plus sign'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithPlusSign
        clinicalData.load(config)
        then:
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects+\\Demographics+\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects+\\Demographics+\\Language++\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects+\\Demographics+\\Language++\\Spain and English and German\\").withPatientCount(1))
    }
}
