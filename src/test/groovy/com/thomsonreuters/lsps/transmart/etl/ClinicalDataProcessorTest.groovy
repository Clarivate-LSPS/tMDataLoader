package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.MappingFileBuilder
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.fixtures.StudyInfo
import groovy.sql.Sql
import spock.lang.Specification

import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime

import static com.thomsonreuters.lsps.transmart.Fixtures.*
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.core.IsNot.not
import static org.junit.Assert.*

class ClinicalDataProcessorTest extends Specification implements ConfigAwareTestCase {
    ClinicalData clinicalData = Fixtures.clinicalData
    private ClinicalDataProcessor _processor

    String studyName = 'Test Study'
    String studyId = 'GSE0'

    void setup() {
        ConfigAwareTestCase.super.setUp()
        runScript('INSERT_ADDITIONAL_DATA.sql')
        runScript('I2B2_LOAD_CLINICAL_DATA.sql')
        runScript('I2B2_DELETE_ALL_DATA.sql')
        runScript('i2b2_create_security_for_trial.sql')
        runScript('I2B2_BUILD_METADATA_XML.sql')
        runScript('I2B2_CREATE_CONCEPT_COUNTS.sql')
        runScript('I2B2_ADD_NODE.sql')
    }

    ClinicalDataProcessor getProcessor() {
        _processor ?: (_processor = new ClinicalDataProcessor(config))
    }

    void testItLoadsAge() {
        setup:
        Study.deleteById(config, clinicalData.studyId)
        Study.deleteByPath(config, "\\Test Studies\\$clinicalData.studyName")

        def result = clinicalData.load(config)

        expect:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(db, hasRecord('i2b2demodata.patient_dimension',
                ['sourcesystem_cd': "${studyId}:HCC827"], [age_in_years_num: 20]))
    }

    def 'it should produce SummaryStatistic.txt'() {
        when:
        Study.deleteById(config, clinicalData.studyId)
        Study.deleteByPath(config, "\\Test Studies\\$clinicalData.studyName")

        def expectedFile = new File(clinicalData.dir, 'ExpectedSummaryStatistic.txt')
        def actualFile = new File(clinicalData.dir, 'SummaryStatistic.txt')
        actualFile.delete()
        def result = clinicalData.load(config)

        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        actualFile.exists()
        actualFile.readLines() == expectedFile.readLines()
    }

    def "it should collect statistic"() {
        setup:
        Study.deleteById(config, clinicalData.studyId)

        def processor = new ClinicalDataProcessor(config)
        database.withSql { sql ->
            processor.processFiles(clinicalData.dir.toPath(), sql as Sql,
                    [name: clinicalData.studyName, node: "\\Test Studies\\${clinicalData.studyName}\\".toString()])
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
                '30-50'                                         : ['HCC2935', 'HCC4006', 'HCC827', 'NCIH3255', 'PC14', 'SKMEL28', 'SW48'],
                'Between 30 to 50'                              : ['HCC2935', 'HCC4006', 'HCC827', 'NCIH3255', 'PC14', 'SKMEL28', 'SW48'],
                'Greater than 30, when "Sex" is equal to "Male"': ['HCC827', 'NCIH3255'],
                '>30'                                           : ['HCC2935', 'HCC4006', 'HCC827', 'NCIH3255', 'PC14', 'SW48'],
                'Greater than or equal to 20'                   : ['NCIH3255', 'SW48'],
                '>=20'                                          : ['NCIH3255', 'SW48'],
                'Lesser than 50'                                : ['SKMEL28'],
                '<50'                                           : ['SKMEL28'],
                'Lesser than or equal to 20'                    : ['HCC4006', 'HCC827', 'NCIH1650', 'NCIH1975', 'PC14', 'SKMEL28'],
                '<=20'                                          : ['HCC4006', 'HCC827', 'NCIH1650', 'NCIH1975', 'PC14', 'SKMEL28']
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
        Study.deleteByPath(config, conceptPath)
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"

        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient('HCC2935').inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M\\'))
        def c = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname like ? || \'%\' ESCAPE \'`\' and c_visualattributes=\'FAS\'', '\\Test Studies\\Test Study\\' as String)
        assertEquals('Count study nodes wrong', 1, c[0] as Integer)
    }

    void testItLoadsDataWithTags() {
        setup:
        def studyInfo = new StudyInfo('GSE0TAG', 'Test Study Tag')
        ClinicalData tagClinicalData = Fixtures.studiesDir.studyDir(studyInfo.name, studyInfo.id).clinicalData
        String conceptPath = "\\Test Studies\\${tagClinicalData.studyName}\\"
        String conceptPathForPatient = conceptPath + tagClinicalData.studyId + '\\eText\\'

        def result = tagClinicalData.reload(config)

        expect:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasPatient('HCC2935').inTrial(studyInfo.id))
        assertThat(sql, hasPatient('2SKMEL28').inTrial(studyInfo.id))
        assertThat(sql, hasNode(conceptPathForPatient + 'tag1\\').withPatientCount(8))
        assertThat(sql, hasNode(conceptPathForPatient + 'tag2\\').withPatientCount(4))
        assertThat(sql, hasNode(conceptPathForPatient + 'tag1 tag and Spain language\\').withPatientCount(1))
        assertThat(sql, hasNode(conceptPathForPatient + 'tag2 tag and English language\\').withPatientCount(2))
    }

    void testItMergesData() {
        expect:
        String conceptPath = "\\Test Studies\\${studyName}\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"
        String subjId = 'HCC2935'
        Study.deleteByPath(config, conceptPath)

        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M\\'))

        processor.process(
                new File(studyDir(studyName, studyId, additionalStudiesDir), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M TEST\\'))

        assertThat(sql, hasNode($/${conceptPath}Biomarker Data\Mutations\TST002 (Entrez ID: 324)\/$))
    }

    def 'it should load study with REPLACE merge mode'() {
        expect:
        String subjId = 'HCC2935'
        String rootConcept = "\\Test Studies\\${studyName}\\"

        String demographicConcept = rootConcept + "Subjects\\Demographics\\"

        processor.process(new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(demographicConcept).withPatientCount(9))

        processor.process(
                new File(studyDir(studyName, studyId, studiesForMerge.replace), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(demographicConcept).withPatientCount(1))
    }

    def 'it should load study with UPDATE merge mode'() {
        expect:
        String subjId = 'HCC2935'
        String rootConcept = "\\Test Studies\\${studyName}\\"

        String maleConcept = rootConcept + "Subjects\\Demographics\\Sex (SEX)\\Male\\"
        String femaleConcept = rootConcept + "Subjects\\Demographics\\Sex (SEX)\\Female\\"
        String languageConcept = rootConcept + "Subjects\\Demographics\\Language\\"
        String ageConcept = rootConcept + "Subjects\\Demographics\\Age (AGE)\\"
        String assessmentDateConcept = rootConcept + "Subjects\\Demographics\\Assessment Date\\"
        String biomarkerConcept = rootConcept + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\ELREA746del\\Variant Type\\DEL\\"

        processor.process(new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(maleConcept).withPatientCount(2))
        assertThat(sql, hasNode(femaleConcept).withPatientCount(5))
        assertThat(sql, hasNode(languageConcept).withPatientCount(3))
        assertThat(sql, hasNode(assessmentDateConcept + "09/15/2014\\"))
        assertThat(sql, hasFact(ageConcept, subjId, 20))
        assertThat(sql, hasNode(biomarkerConcept).withPatientCount(3))

        processor.process(
                new File(studyDir(studyName, studyId, studiesForMerge.update), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(maleConcept).withPatientCount(3))
        assertThat(sql, hasNode(femaleConcept).withPatientCount(4))
        assertThat(sql, hasNode(languageConcept).withPatientCount(4))
        assertThat(sql, hasNode(assessmentDateConcept + "09/15/2015\\"))
        assertThat(sql, hasFact(ageConcept, subjId, 21))
        assertThat(sql, hasNode(biomarkerConcept).withPatientCount(2))
    }

    def 'it should load study with UPDATE VARIABLES merge mode'() {
        expect:
        Study.deleteById(config, 'GSE0SINGLEVN')
        String subjId = 'HCC2935'
        String rootConcept = "\\Test Studies\\${studyName}\\"

        String maleConcept = rootConcept + "Subjects\\Demographics\\Sex (SEX)\\Male\\"
        String femaleConcept = rootConcept + "Subjects\\Demographics\\Sex (SEX)\\Female\\"
        String languageConcept = rootConcept + "Subjects\\Demographics\\Language\\"
        String ageConcept = rootConcept + "Subjects\\Demographics\\Age (AGE)\\"
        String assessmentDateConcept = rootConcept + "Subjects\\Demographics\\Assessment Date\\"
        String biomarkerConcept = rootConcept + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\ELREA746del\\Variant Type\\DEL\\"

        processor.process(new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(maleConcept).withPatientCount(2))
        assertThat(sql, hasNode(femaleConcept).withPatientCount(5))
        assertThat(sql, hasNode(languageConcept).withPatientCount(3))
        assertThat(sql, hasNode(assessmentDateConcept + "09/15/2014\\"))
        assertThat(sql, hasFact(ageConcept, subjId, 20))
        assertThat(sql, hasNode(biomarkerConcept).withPatientCount(3))

        processor.process(
                new File(studyDir(studyName, studyId, studiesForMerge.update_var), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(maleConcept).withPatientCount(3))
        assertThat(sql, hasNode(femaleConcept).withPatientCount(4))
        assertThat(sql, hasNode(languageConcept).withPatientCount(4))
        assertThat(sql, hasNode(assessmentDateConcept + "09/15/2014\\"))
        assertThat(sql, hasFact(ageConcept, subjId, 21))
        assertThat(sql, hasNode(biomarkerConcept).withPatientCount(3))

        String femaleFrenchConcept = '\\Test Studies\\Test Study With Single Visit Name\\Subjects\\Demographics\\Female\\French\\Sex (SEX)\\'
        String maleFrenchConcept = '\\Test Studies\\Test Study With Single Visit Name\\Subjects\\Demographics\\Male\\French\\Sex (SEX)\\'
        def clinicalData = Fixtures.clinicalDataWithSingleVisitName
        clinicalData.load(config)
        assertThat(sql, hasNode(femaleFrenchConcept).withPatientCount(2))

        def studyWithVisitName = 'Test Study With Single Visit Name'
        def newProcessor = new ClinicalDataProcessor(config)
        newProcessor.process(new File(studyDir(studyWithVisitName, 'GSE0SINGLEVN', studiesForMerge.update_var), "ClinicalDataToUpload").toPath(),
                [name: studyWithVisitName, node: "Test Studies\\${studyWithVisitName}".toString()])
        assertThat(sql, hasNode(femaleFrenchConcept).withPatientCount(1))
        assertThat(sql, hasNode(maleFrenchConcept).withPatientCount(1))
    }

    def 'Updates_Variable loading should throw error if find several categorical value on the same'() {
        when:
        String studyName = 'Test Study With Duplicate Category Path'
        String studyId = 'GSE0WDCP'
        Study.deleteById(config, studyId)
        String femaleConcept = '\\Test Studies\\Test Study With Duplicate Category Path\\DupclicateCD\\Female\\'
        def newProcessor = new ClinicalDataProcessor(config)
        newProcessor.process(new File(studyDir(studyName, studyId, studiesForMerge.first_load), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        newProcessor.process(new File(studyDir(studyName, studyId, studiesForMerge.update_var), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        then:
        assertThat(sql, hasNode(femaleConcept).withPatientCount(5))
    }

    def 'it should load study with APPEND merge mode'() {
        expect:
        String subjId = 'HCC2935'
        String rootConcept = "\\Test Studies\\${studyName}\\"

        String maleConcept = rootConcept + "Subjects\\Demographics\\Sex (SEX)\\Male\\"
        String femaleConcept = rootConcept + "Subjects\\Demographics\\Sex (SEX)\\Female\\"
        String languageConcept = rootConcept + "Subjects\\Demographics\\Language\\"
        String ageConcept = rootConcept + "Subjects\\Demographics\\Age (AGE)\\"
        String anotherAgeConcept = rootConcept + "Ages\\Demographics\\Age (AGE)\\"
        String assessmentDateConcept = rootConcept + "Subjects\\Demographics\\Assessment Date\\"

        processor.process(new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(maleConcept).withPatientCount(2))
        assertThat(sql, hasNode(femaleConcept).withPatientCount(5))
        assertThat(sql, hasNode(languageConcept).withPatientCount(3))
        assertThat(sql, hasNode(assessmentDateConcept + "09/15/2014\\"))
        assertThat(sql, hasFact(ageConcept, subjId, 20))
        assertThat(sql, hasFact(anotherAgeConcept, subjId, 20))

        processor.process(
                new File(studyDir(studyName, studyId, studiesForMerge.append), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(maleConcept).withPatientCount(3))
        assertThat(sql, hasNode(femaleConcept).withPatientCount(5))
        assertThat(sql, hasNode(languageConcept).withPatientCount(4))
        assertThat(sql, hasNode(assessmentDateConcept + "09/15/2014\\"))
        assertThat(sql, hasNode(assessmentDateConcept + "09/15/2015\\"))
        assertThat(sql, hasFact(ageConcept, subjId, 21))
        assertThat(sql, hasFact(anotherAgeConcept, subjId, 20))
    }

    def 'it should load study with non-unique column names'() {
        when:
        Study.deleteById(config, 'GSE0NQCN')
        def clinicalData = additionalStudiesDir.studyDir('Test Study With Non Unique Column Names', 'GSE0NQCN').clinicalData
        def successfullyLoaded = clinicalData.load(config + [allowNonUniqueColumnNames: true])
        then:
        noExceptionThrown()
        successfullyLoaded
    }

    def 'it should load category_cd and data_label with plus sign'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithPlusSign
        Study.deleteById(config, clinicalData.studyId)
        def result = clinicalData.load(config)
        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects+\\Demographics+\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects+\\Demographics+\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects+\\Demographics+\\Language++\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects+\\Demographics+\\Language++\\Spain and English and German+\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Spain+English+German+\\Assessment Date\\10/01/2014\\").withPatientCount(1))
    }

    def 'it should load category_cd with terminator'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithTerminator
        Study.deleteById(config, clinicalData.studyId)
        def result = clinicalData.load(config)
        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\").withPatientCount(9))

        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Language\\").withPatientCount(5))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Language\\English\\").withPatientCount(2))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Language\\Spanish\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Language\\French\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Language\\Russian\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Language\\Russian\\v1\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Language\\Russian\\v2\\").withPatientCount(1))

        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Sex (SEX)\\Male\\Spanish\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Sex (SEX)\\Female\\").withPatientCount(2))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Sex (SEX)\\Female\\French\\").withPatientCount(1))
        assertThat(sql, not(hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Sex (SEX)\\Female\\French\\\$\\")))
        assertThat(sql, not(hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Sex (SEX)\\Female\\French\\v1\\")))

        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Age (AGE)\\Male\\").withPatientCount(2))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Age (AGE)\\Female\\").withPatientCount(5))
        assertThat(sql, not(hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Age (AGE)\\Female\\\$\\")))
        assertThat(sql, not(hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\Age (AGE)\\Female\\v1\\")))
    }

    def 'it should load category_cd with data value'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithDataValueInPath
        Study.deleteById(config, clinicalData.studyId)
        def result = clinicalData.load(config)
        def demoPath = "\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics"

        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasNode("$demoPath\\Female\\Baseline\\French\\Sex (SEX)\\").withPatientCount(2))
        assertThat(sql, hasNode("$demoPath\\Female\\Visit 7\\French\\Sex (SEX)\\").withPatientCount(1))
        assertThat(sql, hasNode("$demoPath\\Female\\Baseline\\English\\Sex (SEX)\\").withPatientCount(1))

        assertThat(sql, hasNode("$demoPath\\Age (AGE)\\Baseline\\").withPatientCount(9))
        assertThat(sql, hasNode("$demoPath\\Age (AGE)\\Visit 7\\").withPatientCount(2))

        assertThat(sql, hasNode("$demoPath\\French\\Language\\Baseline\\").withPatientCount(2))
        assertThat(sql, hasNode("$demoPath\\French\\Language\\Visit 7\\").withPatientCount(1))

        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Clinical Data\\Datavalue5\\Baseline\\TAG test 2\\Test data label\\").withPatientCount(1))
    }

    def 'it should remove single visit name by default'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithSingleVisitName
        def result = clinicalData.load(config)
        def demoPath = "\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics"

        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasNode("$demoPath\\Female\\French\\Sex (SEX)\\").withPatientCount(2))
        assertThat(sql, hasNode("$demoPath\\Female\\English\\Sex (SEX)\\").withPatientCount(1))

        assertThat(sql, hasNode("$demoPath\\Russian language\\Age (AGE)\\").withPatientCount(1))
        assertThat(sql, not(hasNode("$demoPath\\Russian language\\Age (AGE)\\Baseline\\")))

        assertThat(sql, hasNode("$demoPath\\Language\\French\\").withPatientCount(2))
        assertThat(sql, not(hasNode("$demoPath\\Language\\French\\Baseline\\")))
    }

    def 'it should always set visit name if option specified'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithSingleVisitName
        config.alwaysSetVisitName = true
        def result = clinicalData.load(config)
        def demoPath = "\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics"

        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasNode("$demoPath\\Female\\Baseline\\French\\Sex (SEX)\\").withPatientCount(2))
        assertThat(sql, hasNode("$demoPath\\Female\\Baseline\\English\\Sex (SEX)\\").withPatientCount(1))

        assertThat(sql, hasNode("$demoPath\\Russian language\\Age (AGE)\\Baseline\\").withPatientCount(1))

        assertThat(sql, hasNode("$demoPath\\Language\\French\\Baseline\\").withPatientCount(2))
    }

    def 'it produces list of duplicates if necessary'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithDuplicatedPatientId
        def expectedFile = new File(clinicalData.dir, 'ExpectedDuplicates.csv')
        def actualFile = new File(clinicalData.dir, 'duplicates.csv')
        actualFile.delete()
        config.checkDuplicates = true
        def result = clinicalData.load(config)
        then:
        assertThat("Clinical data loading should fail", result, equalTo(false))
        actualFile.exists()
        actualFile.readLines().sort() == expectedFile.readLines().sort()
    }

    def 'it does not produces list of duplicates if no duplicates exists'() {
        when:
        def clinicalData = Fixtures.clinicalData
        Study.deleteById(config, clinicalData.studyId)
        def expectedFile = new File(clinicalData.dir, 'ExpectedDuplicates.csv')
        def actualFile = new File(clinicalData.dir, 'duplicates.csv')
        actualFile.delete()
        config.checkDuplicates = true
        def result = clinicalData.load(config)
        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        !actualFile.exists()
    }

    def "it should load different values for different patients in same node"() {
        given:
        Study.deleteById(config, 'DIFVALDIFPATSN')
        def clinicalData = ClinicalData.build('DIFVALDIFPATSN', 'Dif Values for Dif Patients in Same Node') {
            dataFile('TST.txt', ['Visit', 'Duplicates_Cat', 'Duplicates_Num_No_Data_Value', 'Duplicates_Cat_No_Data_Value']) {
                forSubject('TST01') {
                    row 'Baseline', 'Active', '1', 'Active'
                }
                forSubject('TST02') {
                    row 'Baseline', 'Active', '1', 'Inactive'
                }
            }

            mappingFile {
                forDataFile('TST.txt') {
                    mapSpecial 'VISIT_NAME', 3
                    map 'Clinical Data+Status+DATALABEL+DATAVALUE+$', 4, 'Duplicates_Cat'
                    map 'Clinical Data+Status', 5, 'Duplicates_Num_No_Data_Value'
                    map 'Clinical Data+Status+DATALABEL+$', 6, 'Duplicates_Cat_No_Data_Value'
                }
            }
        }

        when:
        def successfullyLoaded = clinicalData.load(config)
        def statusPath = "\\Test Studies\\$clinicalData.studyName\\Clinical Data\\Status"

        then:
        successfullyLoaded
        assertThat(sql, hasNode("$statusPath\\Duplicates Cat\\Active\\").withPatientCount(2))
        assertThat(sql, hasNode("$statusPath\\Duplicates Cat No Data Value\\").withPatientCount(2))
        assertThat(sql, hasFact("$statusPath\\Duplicates Cat No Data Value\\", 'TST01', 'Active'))
        assertThat(sql, hasFact("$statusPath\\Duplicates Cat No Data Value\\", 'TST02', 'Inactive'))
    }

    def "it should load multiple values for same data label"() {
        given:
        Study.deleteById(config, 'GSE0DUPPATHS')
        def clinicalData = ClinicalData.build('GSE0DUPPATHS', 'Test Study With Duplicate Paths') {
            dataFile('AESTATUS.txt', ['System', 'Condition']) {
                forSubject('50015') {
                    row 'Neuro', 'Headache'
                    row 'Neuro', 'Unsteadiness'
                }
            }

            mappingFile {
                forDataFile('AESTATUS.txt') {
                    mapSpecial 'DATA_LABEL', 3
                    mapLabelSource 'Med_His+Active', 4, '3B'
                }
            }
        }
        def medHistoryPath = "\\Test Studies\\$clinicalData.studyName\\Med His"

        when:
        def result = clinicalData.load(config)

        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))

        assertThat(sql, hasNode("$medHistoryPath\\Active\\Neuro\\Headache\\"))
        assertThat(sql, hasNode("$medHistoryPath\\Active\\Neuro\\Unsteadiness\\"))
    }

    def "it should track missing column's value as blank for summary statistic"() {
        given:
        Study.deleteById(config, 'GSE0SS')
        def clinicalData = ClinicalData.build('GSE0SS', 'Test Study Summary Statistic') {
            dataFile('TEST.txt', ['Column', 'Variable']) {
                forSubject('TST1') { row 'Value 1' }
                forSubject('TST2') { row 'Value 2' }
            }

            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars', 3, 'Column')
                    map('Vars', 4, 'Variable', VariableType.Numerical, 'Required; >10')
                }
            }
        }

        when:
        def loadedSuccessfully = clinicalData.load(config)

        then:
        loadedSuccessfully
    }

    private static ClinicalData buildClinicalDataWithTwoVariablesAndMapping(
            @DelegatesTo(MappingFileBuilder) Closure mappingFileBuilder) {
        ClinicalData.build('GSE0SS', 'Test Study Summary Statistic') {
            dataFile('TEST.txt', ['v1', 'v23']) {
                forSubject('TST1') { row 'Value 1', 'Value 2' }
                forSubject('TST2') { row 'Value 3', 'Value 4' }
            }

            mappingFile(mappingFileBuilder)
        }
    }

    def 'it should validate mapping file with duplicate columns'() {
        given:
        def clinicalData = buildClinicalDataWithTwoVariablesAndMapping {
            forDataFile('TEST.txt') {
                map('Vars', 3, 'v1')
                map('Vars', 3, 'v2')
            }
        }

        when:
        clinicalData.load(config)

        then:
        assertThat(sql, hasNode("\\Test Studies\\Test Study Summary Statistic\\Vars\\v1\\"))
        assertThat(sql, hasNode("\\Test Studies\\Test Study Summary Statistic\\Vars\\v2\\"))
    }

    def 'it should validate mapping file with missing column index'() {
        given:
        def clinicalData = buildClinicalDataWithTwoVariablesAndMapping {
            addMappingRow(['TEST.txt', 'Vars', '', 'Var'])
            forDataFile('TEST.txt') {
                map('Vars', 4, 'v2')
            }
        }

        when:
        clinicalData.load(config)

        then:
        thrown(DataProcessingException)
    }

    def 'it should validate mapping file with missing reference to data_label_source'() {
        given:
        def clinicalData = buildClinicalDataWithTwoVariablesAndMapping {
            forDataFile('TEST.txt') {
                mapLabelSource('Vars', 3, 'A')
                mapLabelSource('Vars', 4, '')
            }
        }

        when:
        clinicalData.load(config)

        then:
        thrown(DataProcessingException)
    }

    def 'it should validate that mapping refers to existing file'() {
        given:
        def clinicalData = buildClinicalDataWithTwoVariablesAndMapping {
            forDataFile('TEST2.txt') {
                map('Vars', 3, 'v1')
                map('Vars', 4, 'v2')
            }
        }

        when:
        clinicalData.load(config)

        then:
        thrown(DataProcessingException)
    }

    def 'it should validate mapping column numbers is not out of bound'() {
        given:
        def clinicalData = buildClinicalDataWithTwoVariablesAndMapping {
            forDataFile('TEST.txt') {
                map('Vars', 3, 'v1')
                map('Vars', 6, 'v2')
            }
        }

        when:
        clinicalData.load(config)

        then:
        thrown(DataProcessingException)
    }

    def 'it should validate that tags refers to existing columns'() {
        given:
        def clinicalData = buildClinicalDataWithTwoVariablesAndMapping {
            forDataFile('TEST.txt') {
                map('Vars+$$v3', 3, 'v1')
                map('Vars', 4, 'v2')
            }
        }

        when:
        clinicalData.load(config)

        then:
        thrown(DataProcessingException)
    }

    def 'it should load Serial LDD data'() {
        given:
        Study.deleteById(config, 'GSE0SLDD')
        def clinicalData = ClinicalData.build('GSE0SLDD', 'Test Study With Serial LDD') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timepoint', 3, 'Timepoints', VariableType.Timepoint)
                    map('', 4, 'Timepoint', VariableType.Timepoint)
                    map('Vars', 5, 'Sex')
                }
            }
            dataFile('TEST.txt', ['Days', 'Time point', 'Sex']) {
                forSubject('SUBJ1') {
                    row '20', 'Week -1', 'Female'
                    row '0', 'Baseline', 'Female'
                    row '1', 'Day 1', 'Female'
                    row '7', 'Week 1', 'Female'
                    row '60', 'Month 2', 'Female'
                    row '30', 'months 1', 'Female'
                    row '3', 'days 3', 'Female'
                    row '2', 'day 2', 'Female'
                }
                forSubject('SUBJ2') {
                    row '0', 'Baseline', 'Male'
                    row '30', 'days 3', 'Female'
                    row '20', '2 days', 'Female'
                    row '90', 'Month 3', 'Male'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD\\Vars\\Timepoints"

        when:
        clinicalData.load(config)

        then:
        assertThat db, hasNode("$timepointsPath\\Baseline\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPath\\Day 1\\").withPatientCount(1)
        assertThat db, hasNode("$timepointsPath\\Month 3\\").withPatientCount(1)

        assertThat db, hasTrialVisitDimension("$timepointsPath\\Baseline\\", 'GSE0SLDD:SUBJ1', [
                'rel_time_unit_cd': 'minutes',
                'rel_time_num'    : 0,
                'rel_time_label'  : 'Baseline'
        ])

        assertThat db, hasTrialVisitDimension("$timepointsPath\\Month 2\\", 'GSE0SLDD:SUBJ1', [
                'rel_time_unit_cd': 'minutes',
                'rel_time_num'    : (60 * 24 * 30 * 2),
                'rel_time_label'  : 'Month 2'
        ])
    }

    def 'it should load values with upper and lower case'() {
        setup:
        def customClinicalData = Fixtures.getClinicalData('Test Study With Upper and Lower Case', 'GSE0ULC')
        Study.deleteById(config, customClinicalData.studyId)

        def result = customClinicalData.load(config)

        expect:
        String conceptPath = '\\Test Studies\\Test Study With Upper and Lower Case\\Subjects\\Node\\'
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(db, hasNode("${conceptPath}abilify\\"))
        assertThat(db, hasNode("${conceptPath}Abilify\\"))
        assertThat(db, hasNode("${conceptPath}ABILIFY\\"))
    }

    def 'it should validate load values with non-utf8 symbols'() {
        when:
        def nonUTF8ClinicalData = Fixtures.getClinicalData('Test Study With Non-UTF8 symbols', studyId)
        nonUTF8ClinicalData.load(config)

        then:
        thrown(DataProcessingException)
    }

    def 'it should validate top node was created '() {
        setup:
        Study.deleteByPath(config, '\\Demographics\\')
        clinicalData.load(config)
        clinicalData.copyWithSuffix('SECOND').load(config, '\\Demographics\\Test Study SECOND\\')
        expect:
        assertThat(db, hasRecord('i2b2metadata.i2b2', [c_fullname: '\\Demographics\\'], [:]))
    }

    def 'it should check path when visit_name equal to data_label and data_label is not specified before terminator'() {
        when:
        Study.deleteById(config, 'GSE0REPEATLABPATH')
        def clinicalData = Fixtures.clinicalDataWithTerminatorAndSamePath

        def result = clinicalData.load(config)

        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\").withPatientCount(9))

        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\v1\\").withPatientCount(7))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\v1\\Male\\").withPatientCount(2))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\v1\\Female\\").withPatientCount(5))
        assertThat(sql, hasNode("\\Test Studies\\$clinicalData.studyName\\Subjects\\Demographics\\v2\\").withPatientCount(1))
    }

    def 'it should check error when used wrong mapping file name'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithWrongMappingFileName
        Study.deleteById(config, clinicalData.studyId)
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == 'Mapping file wasn\'t found. Please, check file name.'
    }

    def 'it should check error with long path'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithLongCategoryCD
        Study.deleteById(config, clinicalData.studyId)
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message.contains('CATEGORY_CD is too long (311 > 250) for row [5]')
    }

    def 'it should validate header for non visual symbols'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithNonVisialSymbols
        Study.deleteById(config, clinicalData.studyId)
        clinicalData.load(config)

        then:
        thrown(DataProcessingException)
    }

    def 'it should check on different study id (Var.1 Diff in fill)'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithDifferentStudyID
        Study.deleteById(config, clinicalData.studyId)
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "STUDY_ID differs from previous in 13 line in TST001.txt file."
    }

    def 'it should check on different study id (Var.2 Different in two files)'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithDifferentStudyIDVar2
        Study.deleteById(config, clinicalData.studyId)
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "STUDY_ID differs from previous in 2 line in TST_DEMO.txt file."
    }

    def 'it should not set study_id for upper level directories'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithExtraLevel
        Study.deleteById(config, clinicalData.studyId)
        def result = clinicalData.load(config, "Test Studies\\Extra Level\\")

        then:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(sql, hasNode("\\Test Studies\\Extra Level\\$clinicalData.studyName\\Subjects\\Demographics\\Age (AGE)\\").withPatientCount(9))
        def c = sql.firstRow('select count(*) from concept_dimension where concept_path = ? and sourcesystem_cd is null', '\\Test Studies\\Extra Level\\' as String)
        assertEquals('Count study nodes wrong', 1, c[0] as Integer)
    }

    def 'it should load Serial LDD data with timestamp different baseline in one column'() {
        given:
        Study.deleteById(config, 'GSE0SLDDWTS')
        def clinicalData = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:05'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:05'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:05'
                }
                forSubject('SUBJ3') {
                    row '7', '2000-12-31 11:00', 'Male', '2000-12-31 11:05'
                    row '8', '2000-12-31 11:02', 'Male', '2000-12-31 11:05'
                    row '9', '2000-12-31 11:05', 'Male', '2000-12-31 11:05'
                }
                forSubject('SUBJ4') {
                    row '17', '2000-12-31 13:00', 'Male', '2000-12-31 11:05'
                    row '18', '2000-12-31 13:02', 'Male', '2000-12-31 11:05'
                    row '19', '2000-12-31 13:05', 'Male', '2000-12-31 11:05'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        def loaded = clinicalData.load(config)

        then:
        loaded
        assertThat db, hasNode("$timepointsPath\\Baseline\\").withPatientCount(3)
        assertThat db, hasNode("$timepointsPath\\1 minute\\").withPatientCount(1)

        [["Baseline", 'SUBJ1', 0],
         ["1 minute", 'SUBJ1', 1],
         ["-5 minutes", 'SUBJ3', -5],
         ["2 hours", 'SUBJ4', 120]].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDWTS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }

        assertThat(sql, hasFact("$timepointsPath\\Baseline\\", 'SUBJ1', 0))
        assertThat(sql, hasFact("$timepointsPath\\Baseline\\", 'SUBJ2', 15))
        assertThat(sql, hasFact("$timepointsPath\\Baseline\\", 'SUBJ3', 9))
        assertThat(sql, hasFact("$timepointsPath\\1 minute\\", 'SUBJ1', 10))
        assertThat(sql, hasFact("$timepointsPath\\-5 minutes\\", 'SUBJ3', 7))
        assertThat(sql, hasFact("$timepointsPath\\2 hours\\", 'SUBJ4', 19))
    }

    def 'it should load Serial LDD data with timestamp'() {
        given:
        Study.deleteById(config, 'GSE0SLDDWTS')
        def clinicalData = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        clinicalData.load(config)

        then:
        assertThat db, hasNode("$timepointsPath\\Baseline\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPath\\1 minute\\").withPatientCount(1)

        [
                ["Baseline", 'SUBJ1', 0],
                ["Baseline", 'SUBJ2', 0],
                ["5 minutes", 'SUBJ2', 5],
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDWTS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }
    }

    def 'it should load Serial LDD data With Timestamp and Terminator'() {
        given:
        Study.deleteById(config, 'GSE0SLDDWTS')
        def clinicalData = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp+$', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        clinicalData.load(config)

        then:
        assertThat db, hasNode("$timepointsPath\\Baseline\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPath\\1 minute\\").withPatientCount(1)

        [["Baseline", 'SUBJ1', 0],
         ["5 minutes", 'SUBJ2', 5],
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDWTS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }
    }

    def 'it should load Serial LDD data With Timestamp When All New Timestamps Greater Than Old Min Value'() {
        given:
        Study.deleteById(config, 'GSE0SLDDWTS')
        def clinicalDataFirst = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00'
                }
            }
        }
        clinicalDataFirst.load(config)

        def period1 = '2000-12-31 13:00'
        def period2 = '2000-12-31 14:00'

        def clinicalData = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                addMetaInfo(['MERGE_MODE: APPEND'])
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Race')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp St.1', 'Race', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', period1, 'One', '2000-12-31 12:00'
                    row '12', period2, 'One', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', period1, 'Two', '2000-12-31 12:00'
                    row '13', period2, 'Two', '2000-12-31 12:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        clinicalData.load(config)

        then:
        [["1 hour", 'SUBJ1', 60],
         ["2 hours", 'SUBJ2', 120],
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDWTS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }
    }

    def 'it should load Serial LDD data With Timestamp When Some New Timestamp Lesser Than Old Min Value'() {
        given:
        Study.deleteById(config, 'GSE0SLDDWTS')
        def clinicalDataFirst = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00'
                }
            }
        }
        clinicalDataFirst.load(config)

        def period1 = '2000-12-31 11:00'
        def period2 = '2000-12-31 11:02'
        def period3 = '2000-12-31 11:01'
        def period4 = '2000-12-31 11:05'
        def periodZero = '2000-12-31 12:00'

        def clinicalData = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                addMetaInfo(['MERGE_MODE: APPEND'])
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Race')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp St.1', 'Race', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '5', period1, 'One', '2000-12-31 12:00'
                    row '7', period3, 'One', '2000-12-31 12:00'
                    row '2', period2, 'One', '2000-12-31 12:00'
                    row '4', period4, 'One', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '10', period1, 'Two', '2000-12-31 12:00'
                    row '9', period2, 'Two', '2000-12-31 12:00'
                    row '11', period4, 'Two', '2000-12-31 12:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        clinicalData.load(config)

        then:
        [["Baseline", 'SUBJ1', 0],
         ["-1 hour", 'SUBJ2', -60],
         ["-55 minutes", 'SUBJ2', -55],
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDWTS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }
    }

    def 'it should load Serial LDD data With Timestamp When Some New Timestamp Lesser Than Old Min Value merge mode UPDATE'() {
        given:
        def clinicalDataFirst = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp St.1', 'Race', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00'
                }
            }
        }
        clinicalDataFirst.load(config)

        def period1 = '2000-12-31 11:00'
        def periodMinus58Minutes = '2000-12-31 11:02'
        def period3Minutes = '2000-12-31 12:03'
        def periodMinus55Minutes = '2000-12-31 11:05'
        def periodZero = '2000-12-31 12:00'

        def clinicalData = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                addMetaInfo(['MERGE_MODE: UPDATE'])
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '5', period1, 'Male', '2000-12-31 12:00'
                    row '7', period3Minutes, 'Male', '2000-12-31 12:00'
                    row '2', periodMinus58Minutes, 'Male', '2000-12-31 12:00'
                    row '4', periodMinus55Minutes, 'Male', '2000-12-31 12:00'
                }
                forSubject('SUBJ3') {
                    row '0', period1, 'Male', '2000-12-31 12:00'
                    row '1', periodMinus58Minutes, 'Male', '2000-12-31 12:00'
                    row '2', periodMinus55Minutes, 'Male', '2000-12-31 12:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        clinicalData.load(config)

        then:
        assertThat(sql, hasFact("$timepointsPath\\-1 hour\\", 'SUBJ1', 5))
        assertThat(sql, hasFact("$timepointsPath\\-58 minutes\\", 'SUBJ1', 2))
        assertThat(sql, hasFact("$timepointsPath\\3 minutes\\", 'SUBJ1', 7))
        assertThat(sql, hasFact("$timepointsPath\\-55 minutes\\", 'SUBJ1', 4))
        assertThat(sql, hasFact("$timepointsPath\\Baseline\\", 'SUBJ2', 5))
        assertThat(sql, hasFact("$timepointsPath\\-58 minutes\\", 'SUBJ3', 1))

        [["Baseline", 'SUBJ2', 0],
         ["3 minutes", 'SUBJ1', 3],
         ["-55 minutes", 'SUBJ3', -55],
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDWTS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }

    }

    def 'it should load Serial LDD data With Timestamp  When All New Timestamps Greater Than Old Min Value merge mode UPDATE'() {
        given:
        def periodZero = '2000-12-31 12:00'
        def clinicalDataFirst = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '0', periodZero, 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '5', periodZero, 'Male', '2000-12-31 12:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00'
                }
            }
        }
        clinicalDataFirst.load(config)

        def period1Hour = '2000-12-31 13:00'
        def period2 = '2000-12-31 13:02'
        def period3 = '2000-12-31 13:01'
        def period4 = '2000-12-31 13:05'

        def clinicalData = ClinicalData.build('GSE0SLDDWTS', 'Test Study With Serial LDD with timestamp') {
            mappingFile {
                addMetaInfo(['MERGE_MODE: UPDATE'])
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp St.1', 'Sex', 'Baseline']) {
                forSubject('SUBJ1') {
                    row '5', period1Hour, 'Male', '2000-12-31 12:00'
                    row '7', period3, 'Male', '2000-12-31 12:00'
                    row '2', period2, 'Male', '2000-12-31 12:00'
                    row '4', period4, 'Male', '2000-12-31 12:00'
                }
                forSubject('SUBJ3') {
                    row '0', period1Hour, 'Male', '2000-12-31 12:00'
                    row '1', period2, 'Male', '2000-12-31 12:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        clinicalData.load(config)

        then:
        assertThat(sql, hasFact("$timepointsPath\\1 hour\\", 'SUBJ1', 5))
        assertThat(sql, hasFact("$timepointsPath\\1 hour 2 minutes\\", 'SUBJ1', 2))
        assertThat(sql, hasFact("$timepointsPath\\1 hour 1 minute\\", 'SUBJ1', 7))
        assertThat(sql, hasFact("$timepointsPath\\1 hour 5 minutes\\", 'SUBJ1', 4))
        assertThat(sql, hasFact("$timepointsPath\\Baseline\\", 'SUBJ2', 5))
        assertThat(sql, hasFact("$timepointsPath\\1 hour\\", 'SUBJ3', 0))

        [["Baseline", 'SUBJ2', 0],
         ["1 hour", 'SUBJ1', 60],
         ["1 hour 5 minutes", 'SUBJ1', 65],
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDWTS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }
    }

    def 'it should load Serial LDD data with timestamp with two timestamp column'() {
        given:
        Study.deleteById(config, 'GSE0SLDDW2TS')
        def clinicalData = ClinicalData.build('GSE0SLDDW2TS', 'Test Study With Serial LDD with timestamp with two column') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                    map('Other+DATALABEL+$$Timestamp2', 7, 'Timestamp2', 'Baseline', VariableType.Timestamp)
                    map('', 8, 'Timestamp2', '', VariableType.Timestamp)
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline', 'Count', 'Timestamp2']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00', '1', '2000-12-31 14:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00', '2', '2000-12-31 14:01'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00', '3', '2000-12-31 14:02'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00', '4', '2000-12-31 14:03'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00', '1', '2000-12-31 14:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00', '10', '2000-12-31 14:01'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00', '100', '2000-12-31 14:02'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp with two column\\Vars\\Timestamp"
        String timepointsPathForSecond = "\\Test Studies\\Test Study With Serial LDD with timestamp with two column\\Other\\Timestamp2"

        when:
        clinicalData.load(config)

        then:
        assertThat db, hasNode("$timepointsPath\\Baseline\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPath\\1 minute\\").withPatientCount(1)
        assertThat db, hasNode("$timepointsPathForSecond\\2 hours\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPathForSecond\\2 hours 3 minutes\\").withPatientCount(1)

        [["Baseline", 'SUBJ1', 0],
         ["5 minutes", 'SUBJ2', 5]]
                .each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDW2TS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }
        [["2 hours", 'SUBJ2', 120],
         ["2 hours 3 minutes", 'SUBJ1', 123]
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPathForSecond\\${it[0]}\\", "GSE0SLDDW2TS:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }
    }

    def 'it should load Serial LDD data with timestamp with two timestamp and baseline'() {
        given:
        Study.deleteById(config, 'GSE0SLDDW2TS2B')
        def clinicalData = ClinicalData.build('GSE0SLDDW2TS2B', 'Test Study With Serial LDD with two timestamp and baseline') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Vars+DATALABEL+$$Timestamp', 3, 'Timestamp', 'Baseline', VariableType.Timestamp)
                    map('', 4, 'Timestamp', '', VariableType.Timestamp)
                    map('Vars', 5, 'Sex')
                    map('', 6, 'Baseline')
                    map('Other+DATALABEL+$$Timestamp2', 7, 'Timestamp2', 'Baseline2', VariableType.Timestamp)
                    map('', 8, 'Timestamp2', '', VariableType.Timestamp)
                    map('', 9, 'Baseline2')
                }
            }
            dataFile('TEST.txt', ['Days', 'Timestamp', 'Sex', 'Baseline', 'Count', 'Timestamp2', 'Baseline2']) {
                forSubject('SUBJ1') {
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00', '1', '2000-12-31 14:00', '2000-12-31 13:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00', '2', '2000-12-31 14:01', '2000-12-31 13:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00', '3', '2000-12-31 14:02', '2000-12-31 13:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00', '4', '2000-12-31 14:03', '2000-12-31 13:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00', '1', '2000-12-31 14:00', '2000-12-31 13:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00', '10', '2000-12-31 14:01', '2000-12-31 13:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00', '100', '2000-12-31 14:02', '2000-12-31 13:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with two timestamp and baseline\\Vars\\Timestamp"
        String timepointsPathForSecond = "\\Test Studies\\Test Study With Serial LDD with two timestamp and baseline\\Other\\Timestamp2"

        when:
        def loaded = clinicalData.load(config)

        then:
        loaded
        assertThat db, hasNode("$timepointsPath\\Baseline\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPath\\1 minute\\").withPatientCount(1)
        assertThat db, hasNode("$timepointsPathForSecond\\1 hour\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPathForSecond\\1 hour 3 minutes\\").withPatientCount(1)

        [["Baseline", 'SUBJ1', 0],
         ["2 minutes", 'SUBJ2', 2]
        ].each {
            assertThat db, hasTrialVisitDimension("$timepointsPath\\${it[0]}\\", "GSE0SLDDW2TS2B:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }

        [["1 hour", 'SUBJ2', 60],
         ["1 hour 3 minutes", 'SUBJ1', 63]].each {
            assertThat db, hasTrialVisitDimension("$timepointsPathForSecond\\${it[0]}\\", "GSE0SLDDW2TS2B:${it[1]}", [
                    'rel_time_unit_cd': 'minutes',
                    'rel_time_num'    : it[2],
                    'rel_time_label'  : it[0]
            ])
        }

    }

    def checkMetaDataXMLForTimestamp(path, value, datetime) {
        hasRecord("i2b2", [c_fullname: "$path\\$datetime\\"], [
                c_metadataxml: {
                    def metadata = new XmlParser().parseText(it as String)
                    assertThat(metadata.Oktousevalues.text(), equalTo('Y'))
                    assertThat(metadata.SeriesMeta.Value.text(), equalTo(value))
                    assertThat(metadata.SeriesMeta.Unit.text(), equalTo('minutes'))
                    assertThat(metadata.SeriesMeta.DisplayName.text(), equalTo(datetime))
                    true
                }
        ])
    }

    def 'It should load data into transmart 17.1 new tables'() {
        given:
        Study.deleteById(config, studyId)
        config.securitySymbol = 'Y'

        when:
        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        then:
        def studyNum
        sql.execute("select study_num from i2b2demodata.study where study_id = ? ", [studyId as String],
                { isResultSet, row -> studyNum = row.study_num })
        assertThat(db, hasRecord(['study_id': studyNum[0]], 'i2b2metadata.study_dimension_descriptions'))
        assertThat(db, hasRecord(['study_num': studyNum[0]], 'i2b2demodata.trial_visit_dimension'))
        assertThat(db, hasRecord(['sourcesystem_cd': "${studyId}"], 'i2b2demodata.visit_dimension'))
        assertThat(db, hasRecord('i2b2demodata.visit_dimension',
                ['sourcesystem_cd': "${studyId}"], [start_date: Timestamp.valueOf(LocalDateTime.parse("0001-01-01T00:00:00"))]))

        assertThat(db, hasRecord('i2b2metadata.i2b2_secure',
                ['sourcesystem_cd': "${studyId}"], [secure_obj_token: "EXP:${studyId}"]))
        assertThat(db, hasRecord('i2b2demodata.study',
                ['study_id': "${studyId}"], [secure_obj_token: "EXP:${studyId}"]))

    }

    def 'It should return patients as API v2'() {
        given:
        Study.deleteById(config, studyId)

        when:
        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        then:
        def cnt
        sql.execute("""
                select count(*) as cnt from  
                  i2b2demodata.observation_fact 
                where trial_visit_num in (
                  select trial_visit_num from i2b2demodata.trial_visit_dimension WHERE study_num in (
                    select study_num from i2b2demodata.study where study_id = ?
                  )
                )
            """, [studyId as String],
                { isResultSet, row -> cnt = row[0].cnt })
        assertTrue(cnt > 0)
    }

    def 'It should check observation as API v2'() {
        given:
        Study.deleteById(config, studyId)

        when:
        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        then:
        def cnt
        sql.execute("""
                select count(*) as cnt from 
                  i2b2demodata.observation_fact this_ 
                  inner join i2b2demodata.trial_visit_dimension trialvisit1_ on this_.trial_visit_num=trialvisit1_.trial_visit_num 
                where 
                    trialvisit1_.study_num in (
                      select study_.study_num as y0_ from i2b2demodata.study study_ where study_.study_id= ?
                    )
            """, [studyId as String],
                { isResultSet, row -> cnt = row[0].cnt })
        assertTrue(cnt > 0)
    }

    def 'It should load study with start date, end date, instance and visit fields'() {
        given:
        def studyTr171Id = "TR171"
        def studyTr171Name = 'Test Study For Transmart-17-1'
        Study.deleteById(config, studyTr171Id)

        when:
        def load = processor.process(
                new File(studyDir(studyTr171Name, studyTr171Id), "ClinicalDataToUpload").toPath(),
                [name: studyTr171Name, node: "Test Studies\\${studyTr171Name}".toString()])
        then:
        assertTrue(load)

        assertThat(db, hasFactAttribute("${studyTr171Id}:OBS336-201_01", "\\Test Studies\\${studyTr171Name}\\PKConc\\Timepoint Hrs.\\", 1,
                [
                        'start_date': Timestamp.valueOf(LocalDate.parse("2016-03-02").atStartOfDay()),
                        'end_date'  : Timestamp.valueOf(LocalDate.parse("2016-03-03").atStartOfDay())
                ]
        ))
        assertThat(db, hasFactAttribute("${studyTr171Id}:OBS336-201_03", "\\Test Studies\\${studyTr171Name}\\Demography\\Sex\\F\\", 1,
                [
                        'start_date': Timestamp.valueOf(LocalDate.parse("2016-03-11").atStartOfDay())
                ]
        ))
        assertThat(db, hasFactAttribute("${studyTr171Id}:OBS336-201_02", "\\Test Studies\\${studyTr171Name}\\PKConc\\Timepoint Hrs.\\", 1,
                [
                        'start_date': Timestamp.valueOf(java.time.LocalDateTime.parse("2016-03-02T08:13:00")),
                        'end_date'  : Timestamp.valueOf(LocalDate.parse("2016-03-03").atStartOfDay())
                ]
        ))

        assertThat(db, hasFactAttribute("${studyTr171Id}:OBS336-201_07", "\\Test Studies\\${studyTr171Name}\\PKConc\\Timepoint Hrs.\\", 1,
                [
                        'end_date': Timestamp.valueOf(LocalDateTime.parse("2016-03-03T14:34:19"))
                ]
        ))

        assertThat(db, hasVisitDimension('OBS336-201_02', studyTr171Id,
                [start_date: Timestamp.valueOf(LocalDateTime.parse("2016-03-02T00:00"))]))
    }

    def 'Load sample data with TRIAL_VISIT_TIME and TRIAL_VISIT_UNIT'() {
        given:
        def currentStudyId = "TVTTVL"
        def currentStudyName = 'Test Data With Time And Unit Keywords'
        Study.deleteById(config, currentStudyId)

        when:
        def load = processor.process(
                new File(studyDir(currentStudyName, currentStudyId), "ClinicalDataToUpload").toPath(),
                [name: currentStudyName, node: "Test Studies\\${currentStudyName}".toString()])
        then:
        assertTrue(load)
        assertThat(db, hasTrialVisitDimension("\\Test Studies\\${currentStudyName}\\Vital Signs\\Heart Rate\\",
                "${currentStudyId}:-41", 'Week 1', ['rel_time_unit_cd': 'days', 'rel_time_num': 7]))
        assertThat(db, hasTrialVisitDimension("\\Test Studies\\${currentStudyName}\\Vital Signs\\Heart Rate\\",
                "${currentStudyId}:-51", 'Week 3', ['rel_time_unit_cd': 'days', 'rel_time_num': 21]))
    }

    def 'Load sample data with concept_cd'() {
        given:
        def studyConceptId = "GSECONCEPTCD"
        def studyConceptName = 'Test Data With Concept_cd'
        Study.deleteById(config, studyConceptId)
        Study.deleteCross(config, '\\Vital\\')

        when:
        def load = processor.process(
                new File(studyDir(studyConceptName, studyConceptId), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName, node: "Test Studies\\${studyConceptName}".toString()])
        then:
        def crossNode = "Vital\\Node 1\\Node 2\\Flag\\"
        assertTrue(load)

        assertThat(db, hasFactAttribute("${studyConceptId}:-61", "\\Test Studies\\${studyConceptName}\\${crossNode}",
                [
                        'start_date': Timestamp.valueOf(java.time.LocalDateTime.parse("0001-01-01T00:00:00")),
                        'concept_cd': 'VSIGN:FLAG'
                ]
        ))
        assertThat(sql, hasNode("\\${crossNode}"))

        assertThat(db, hasFactAttribute("${studyConceptId}:-61", "\\Test Studies\\${studyConceptName}\\Demographics\\Age\\",
                [
                        'concept_cd': 'DM:AGE'
                ]))

        assertThat(db, hasFactAttribute("${studyConceptId}:-61", "\\Test Studies\\${studyConceptName}\\Demographics\\Race\\", [
                'concept_cd': 'DM:RACE'
        ]))

        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Race\\").withPatientCount(4))
        assertThat(sql, hasFact("\\Test Studies\\${studyConceptName}\\Demographics\\Race\\", '-61', 'Caucasian'))
        assertThat(sql, hasFact("\\Test Studies\\${studyConceptName}\\Demographics\\Race\\", '-51', 'Latino'))
        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Male\\").withPatientCount(2))
        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Female\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Unknown\\").withPatientCount(1))

        assertThat(sql, hasFactAttribute("${studyConceptId}:-51", "\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Male\\", 1, [
                'concept_cd': 'DM:GENDER:M'
        ]))

        assertThat(sql, hasFactAttribute("${studyConceptId}:-41", "\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Female\\", 1, [
                'concept_cd': 'DM:GENDER:F'
        ]))

        assertThat(sql, hasFactAttribute("${studyConceptId}:TESTME", "\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Unknown\\", 1, [
                'concept_cd': 'DM:GENDER:UNKNOWN'
        ]))

        assertThat(sql, hasRecord(['c_fullname': "\\Vital\\Node 1\\Node 2\\Flag\\"], "i2b2metadata.i2b2_secure"))
        assertThat(sql, hasRecord(['c_fullname': "\\Vital\\Node 1\\Node 2\\"], "i2b2metadata.i2b2_secure"))
        assertThat(sql, hasRecord(['c_fullname': "\\Vital\\Node 1\\"], "i2b2metadata.i2b2_secure"))
        assertThat(sql, hasRecord(['c_fullname': "\\Vital\\"], "i2b2metadata.i2b2_secure"))
    }

    def 'Should check throw exception if time data is not valid: time is set, unit isn\'t'() {
        given:
        Study.deleteById(config, 'GSE0WTDU1')
        def clinicalData = ClinicalData.build('GSE0WTDU1', 'Test Study With Wrong Setting Time Data') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Demography', 3, 'Demography')
                    map('Demography', 4, 'TRIAL_VISIT_TIME')
                    map('Demography', 5, 'TRIAL_VISIT_UNIT')
                }
            }
            dataFile('TEST.txt', ['Demography', 'Time', 'Unit']) {
                forSubject('SUBJ1') {
                    row 'Male', '5', 'Days'
                }
                forSubject('SUBJ2') {
                    row 'Female', '', 'Days'
                }
            }
        }

        when:
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "Unit value is specified, but time value isn't. Please, check line 3 in TEST.txt file."

    }

    def 'Should check throw exception if time data is not valid: time isn\'t set, unit is set'() {
        given:
        Study.deleteById(config, 'GSE0WTDU2')
        def clinicalData = ClinicalData.build('GSE0WTDU2', 'Test Study With Wrong Setting Time Data') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Demography', 3, 'Demography')
                    map('Demography', 4, 'TRIAL_VISIT_TIME')
                    map('Demography', 5, 'TRIAL_VISIT_UNIT')
                }
            }
            dataFile('TEST.txt', ['Demography', 'Time', 'Unit']) {
                forSubject('SUBJ1') {
                    row 'Male', '5', 'Days'
                }
                forSubject('SUBJ2') {
                    row 'Female', '3', ''
                }
            }
        }

        when:
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "Time value is specified, but unit value isn't. Please, check line 3 in TEST.txt file."
    }

    def 'Should check throw exception if time data is not valid: same label diff time unit pair'() {
        given:
        Study.deleteById(config, 'GSE0WTDU3')
        def clinicalData = ClinicalData.build('GSE0WTDU3', 'Test Study With Wrong Setting Time Data') {
            mappingFile {
                forDataFile('TEST.txt') {
                    mapSpecial('INSTANCE_NUM', 3)
                    mapSpecial('TRIAL_VISIT_TIME', 4)
                    mapSpecial('TRIAL_VISIT_UNIT', 5)
                    mapSpecial('TRIAL_VISIT_LABEL', 6)
                    map('Demography', 7, 'Demography')
                }
            }
            dataFile('TEST.txt', ['instance_num', 'Time', 'Unit', 'label', 'Demography']) {
                forSubject('SUBJ1') {
                    row '1', '0', 'Days', 'Baseline', 'Male'
                    row '2', '1', 'Days', '1 Day', 'Male'
                    row '3', '7', 'Days', '1 Week', 'Male'
                }
                forSubject('SUBJ2') {
                    row '1', '0', 'Days', 'Baseline', 'Female'
                    row '2', '1', 'Days', '1 Day', 'Female'
                    row '3', '8', 'Days', '1 Week', 'Female'
                }
            }
        }

        when:
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == 'There was a previous row with the same LABEL (1 Week) but different (TIME, UNIT) [7, Days] not equal [8, Days]. Please, check line 7 in TEST.txt file.'
    }

    def 'Should check throw exception if time data is not valid: same time unit pair, but different label '() {
        given:
        Study.deleteById(config, 'GSE0WTDU3')
        def clinicalData = ClinicalData.build('GSE0WTDU3', 'Test Study With Wrong Setting Time Data') {
            mappingFile {
                forDataFile('TEST.txt') {
                    mapSpecial('INSTANCE_NUM', 3)
                    mapSpecial('TRIAL_VISIT_TIME', 4)
                    mapSpecial('TRIAL_VISIT_UNIT', 5)
                    mapSpecial('TRIAL_VISIT_LABEL', 6)
                    map('Demography', 7, 'Demography')
                }
            }
            dataFile('TEST.txt', ['instance_num', 'Time', 'Unit', 'label', 'Demography']) {
                forSubject('SUBJ1') {
                    row '1', '0', 'Days', 'Baseline', 'Male'
                    row '2', '1', 'Days', '1 Day', 'Male'
                    row '3', '7', 'Days', '1 Week', 'Male'
                }
                forSubject('SUBJ2') {
                    row '1', '0', 'Days', 'Baseline', 'Female'
                    row '2', '1', 'Days', '1 Day', 'Female'
                    row '3', '7', 'Days', '7 days', 'Female'
                }
            }
        }

        when:
        clinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == 'There was a previous row with the same (TIME, UNIT) ([7, Days]) but different LABEL (1 Week) not equal (7 days). Please, check line 7 in TEST.txt file.'
    }

    def 'Should check throw duplicate concept_cd but different path'() {
        given:
        def studyConceptId = "GSECONCEPTCD"
        def studyConceptName = 'Test Data With Concept_cd'
        Study.deleteById(config, studyConceptId)
        Study.deleteById(config, studyConceptId + '2')
        Study.deleteByPath(config, '\\Vital\\')
        processor.process(
                new File(studyDir(studyConceptName, studyConceptId), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName, node: "Test Studies\\${studyConceptName}".toString()])

        when:
        processor.usedStudyId = ''
        def result = processor.process(
                new File(studyDir(studyConceptName, studyConceptId + '2'), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName + " Another", node: "Test Studies\\${studyConceptName} Another".toString()])

        then:
        assertThat("Should check throw duplicate concept_cd", result, equalTo(false))
    }

    def 'Should check throw duplicate path but different concept_cd'() {
        given:
        def studyConceptId = "GSECONCEPTCD"
        def studyConceptName = 'Test Data With Concept_cd'
        Study.deleteById(config, studyConceptId)
        Study.deleteByPath(config, '\\Vital\\')
        processor.process(
                new File(studyDir(studyConceptName, studyConceptId), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName, node: "Test Studies\\${studyConceptName}".toString()])

        when:
        processor.usedStudyId = ''
        def result = processor.process(
                new File(studyDir(studyConceptName, studyConceptId, additionalStudiesDir), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName, node: "Test Studies\\${studyConceptName}".toString()])

        then:
        assertThat("Should check throw duplicate concept_cd", result, equalTo(false))
    }

    def 'Should check load cross study by exist another study path'() {
        given:
        def studyConceptId = "GSECONCEPTCD"
        def studyConceptName = 'Test Data With ConceptCD'
        def topFolder = 'Test Studies2'
        Study.deleteById(config, studyConceptId)
        Study.deleteById(config, 'GSECSSP')
        Study.deleteByPath(config, '\\Vital\\')
        processor.process(
                new File(studyDir('Test Data With Concept_cd', studyConceptId), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName, node: "${topFolder}\\${studyConceptName}".toString()])

        when:
        processor.usedStudyId = ''
        def result = processor.process(
                new File(studyDir('Test Data With Cross Study By Same Path', 'GSECSSP', additionalStudiesDir), 'ClinicalDatatoUpload').toPath(),
                [name: 'Test Data With Cross Study By Same Path', node: "${topFolder}\\Test Data With Cross Study By Same Path".toString()]
        )
        then:
        assertThat("Should check parent nodes", result, equalTo(false))
    }

    def 'Should check load share patients to patient_mapping'() {
        given:
        def studyName = 'Test Study With Share Patients'
        def currentStudyId = 'GSE0WSP'
        def patientShareString = "SHARED_TEST"

        when:
        def result = processor.process(
                new File(studyDir(studyName, currentStudyId), 'ClinicalDataToUpload').toPath(),
                [name: studyName, node: "\\Test Studies2\\${studyName}".toString()]
        )

        then:
        assertTrue('Upload didn\'t success', result)
        assertThat(db, hasRecord(['patient_ide': "${patientShareString}:SW48"], 'i2b2demodata.patient_mapping'))

        cleanup:
        Study.deleteById(config, currentStudyId)
        sql.executeUpdate("DELETE FROM i2b2demodata.patient_dimension WHERE sourcesystem_cd like ?", patientShareString + ':%')
        sql.executeUpdate("DELETE FROM i2b2demodata.patient_mapping WHERE patient_ide like ?", patientShareString + ':%')
    }

    def 'Should check load two study with share patients'() {
        given:
        def studyNameFirst = 'Test Study With Share Patients'
        def studyNameSecond = 'Test Study With Share Patients Second'
        def firstStudyId = 'GSE0WSP'
        def secondStudyId = 'GSE0WSPSECOND'
        def patientShareString = "SHARED_TEST"

        when:
        def resultFirst = processor.process(
                new File(studyDir(studyNameFirst, firstStudyId), 'ClinicalDataToUpload').toPath(),
                [name: studyNameFirst, node: "\\Test Studies2\\${studyNameFirst}".toString()])

        processor.usedStudyId = ''
        def resultSecond = processor.process(
                new File(studyDir(studyNameSecond, secondStudyId), 'ClinicalDataToUpload').toPath(),
                [name: studyNameSecond, node: "\\Test Studies2\\${studyNameSecond}".toString()]
        )

        then:
        assertTrue('First study didn\'t upload', resultFirst)
        assertTrue('Second study didn\'t upload', resultSecond)
        assertThat(db, hasRecord(['patient_ide': "${patientShareString}:SW48"], 'i2b2demodata.patient_mapping'))
        assertThat(sql, hasRecord(['c_fullname': "\\Test Studies2\\${studyNameFirst}\\Subjects\\Demographics\\Age (AGE)\\"], "i2b2metadata.i2b2"))
        assertThat(sql, hasRecord(['c_fullname': "\\Test Studies2\\${studyNameSecond}\\Subjects\\Demographics\\Age (AGE)\\"], "i2b2metadata.i2b2"))
        def patientCount = sql.firstRow("""
                select count(*) as cnt from  
                  i2b2demodata.patient_dimension 
                where sourcesystem_cd = ?
            """, ["$patientShareString:SW48".toString()])
        assertTrue(patientCount.cnt == 1)

        def patientMapping = sql.firstRow("""
                SELECT COUNT(*) as cnt from 
                  i2b2demodata.patient_mapping  pm
                  inner JOIN
                  i2b2demodata.patient_dimension pd
                  ON pm.patient_num = pd.patient_num
                  where pd.sourcesystem_cd = ?                                                
            """, ["$patientShareString:SW48".toString()])
        assertTrue(patientMapping.cnt == 1)
    }

    def 'It should check strong check patients'() {
        given:
        def clinicalData = ClinicalData.build('GSE0PT1', 'Test Study With Patient First Study') {
            mappingFile {
                addMetaInfo(['SHARED_PATIENTS: PT_TEST'])
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PT02') {
                    row '50', 'Female', 'Asian'
                }
                forSubject('PT03') {
                    row '50', 'Male', 'Asian'
                }
            }
        }
        def clinicalDataSecond = ClinicalData.build('GSE0PT2', 'Test Study With Patient Second Study') {
            mappingFile {
                addMetaInfo(['SHARED_PATIENTS: PT_TEST'])
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PT02') {
                    row '30', 'Female', 'Asian'
                }
                forSubject('PT03') {
                    row '50', 'Female', 'Asian'
                }
            }
        }

        when:
        def result1 = clinicalData.load(config)

        config.strongCheck = true
        def result2 = clinicalDataSecond.load(config)

        then:
        assertTrue(result1)
        assertFalse(result2)

        def checkPatient = sql.firstRow("""
                SELECT sex_cd, age_in_years_num, race_cd, pd.sourcesystem_cd from 
                  i2b2demodata.patient_mapping  pm
                  inner JOIN
                  i2b2demodata.patient_dimension pd
                  ON pm.patient_num = pd.patient_num
                  where pd.sourcesystem_cd = ?                                                
            """, [
                "PT_TEST:PT02".toString()
        ])
        assertTrue('Sex must not to change', checkPatient.sex_cd == 'Female')
        assertTrue('Age must not to change', checkPatient.age_in_years_num == 50)
        assertTrue('Race must not to change', checkPatient.race_cd == 'Asian')

        cleanup:
        Study.deleteById(config, 'GSE0PT1')
        Study.deleteById(config, 'GSE0PT2')
    }

    def 'It should check update patients without strong checking'() {
        given:
        def clinicalData = ClinicalData.build('GSE0PT1', 'Test Study With Patient First Study') {
            mappingFile {
                addMetaInfo(['SHARED_PATIENTS: PT_TEST'])
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PT02') {
                    row '50', 'Female', 'Asian'
                }
                forSubject('PT03') {
                    row '50', 'Male', 'Asian'
                }
            }
        }
        def clinicalDataSecond = ClinicalData.build('GSE0PT2', 'Test Study With Patient Second Study') {
            mappingFile {
                addMetaInfo(['SHARED_PATIENTS: PT_TEST'])
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PT02') {
                    row '30', 'Female', 'Asian'
                }
                forSubject('PT03') {
                    row '50', 'Female', 'Asian'
                }
            }
        }

        when:
        def result1 = clinicalData.load(config)

        def result2 = clinicalDataSecond.load(config)

        then:
        assertTrue(result1)
        assertTrue(result2)

        def checkPatient = sql.firstRow("""
                SELECT sex_cd, age_in_years_num, race_cd, pd.sourcesystem_cd from 
                  i2b2demodata.patient_mapping  pm
                  inner JOIN
                  i2b2demodata.patient_dimension pd
                  ON pm.patient_num = pd.patient_num
                  where pd.sourcesystem_cd = ?                                                
            """, [
                "PT_TEST:PT02".toString()
        ])
        assertTrue('Sex must not to change', checkPatient.sex_cd == 'Female')
        assertTrue('Age must not to change', checkPatient.age_in_years_num == 30)
        assertTrue('Race must not to change', checkPatient.race_cd == 'Asian')

        cleanup:
        Study.deleteById(config, 'GSE0PT1')
        Study.deleteById(config, 'GSE0PT2')

    }

    def 'It should upload studies with same study id and share patient id'() {
        given:
        def clinicalData = ClinicalData.build('GSE0PTT', 'Test Study With Share Patients') {
            mappingFile {
                addMetaInfo(['SHARED_PATIENTS: PTT_TEST'])
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PTT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PTT02') {
                    row '50', 'Female', 'Asian'
                }
                forSubject('PTT03') {
                    row '50', 'Male', 'Asian'
                }
                forSubject('PTT04') {
                    row '10', 'Male', 'Asian'
                }
            }
        }

        def clinicalDataSecond = ClinicalData.build('PTT_TEST', 'Test Study With PTT_TEST ID') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PTT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PTT02') {
                    row '50', 'Female', 'Asian'
                }
                forSubject('PTT03') {
                    row '50', 'Male', 'Asian'
                }
            }
        }

        when:
        def result1 = clinicalData.load(config)
        def result2 = clinicalDataSecond.load(config)

        then:
        assertTrue(result1)
        assertFalse(result2)

        def checkPatient = sql.firstRow("""
                SELECT count(*) as cnt from 
                  i2b2demodata.patient_mapping  pm
                  inner JOIN
                  i2b2demodata.patient_dimension pd
                  ON pm.patient_num = pd.patient_num
                  where pd.sourcesystem_cd in (?, ?, ?, ?)                                                
            """, [
                "PTT_TEST:PTT01".toString(),
                "PTT_TEST:PTT02".toString(),
                "PTT_TEST:PTT03".toString(),
                "PTT_TEST:PTT04".toString()
        ])
        assertEquals('Count of patients is wrong', 4, checkPatient.cnt)

        cleanup:
        Study.deleteById(config, 'GSE0PTT')
        Study.deleteById(config, 'PTT_TEST')
    }

    def 'It should upload studies with same share patient id and study id'() {
        given:
        def clinicalData = ClinicalData.build('PTT_TEST2', 'Test Study With PTT_TEST2 ID') {
            mappingFile {
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PTT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PTT02') {
                    row '50', 'Female', 'Asian'
                }
                forSubject('PTT03') {
                    row '50', 'Male', 'Asian'
                }
            }
        }

        def clinicalDataSecond = ClinicalData.build('GSE0PTT2', 'Test Study With Share Patients') {
            mappingFile {
                addMetaInfo(['SHARED_PATIENTS: PTT_TEST2'])
                forDataFile('TEST.txt') {
                    map('Subjects+Demographics', 3, 'Age (AGE)')
                    map('Subjects+Demographics', 4, 'Sex (SEX)')
                    map('Subjects+Demographics', 5, 'Race (RACE)')
                }
            }
            dataFile('TEST.txt', ['age', 'sex', 'race']) {
                forSubject('PTT01') {
                    row '30', 'Male', 'Europian'
                }
                forSubject('PTT02') {
                    row '50', 'Female', 'Asian'
                }
                forSubject('PTT03') {
                    row '50', 'Male', 'Asian'
                }
                forSubject('PTT04') {
                    row '10', 'Male', 'Asian'
                }
            }
        }


        when:
        def result1 = clinicalData.load(config)
        def result2 = clinicalDataSecond.load(config)

        then:
        assertTrue(result1)
        assertFalse(result2)

        def checkPatient = sql.firstRow("""
                SELECT count(*) as cnt from 
                  i2b2demodata.patient_mapping  pm
                  inner JOIN
                  i2b2demodata.patient_dimension pd
                  ON pm.patient_num = pd.patient_num
                  where pd.sourcesystem_cd in (?, ?, ?, ?)                                                
            """, [
                "PTT_TEST2:PTT01".toString(),
                "PTT_TEST2:PTT02".toString(),
                "PTT_TEST2:PTT03".toString(),
                "PTT_TEST2:PTT04".toString()
        ])
        assertEquals('Count of patients is wrong', 0, checkPatient.cnt)

        cleanup:
        Study.deleteById(config, 'GSE0PTT2')
        Study.deleteById(config, 'PTT_TEST2')
    }

}