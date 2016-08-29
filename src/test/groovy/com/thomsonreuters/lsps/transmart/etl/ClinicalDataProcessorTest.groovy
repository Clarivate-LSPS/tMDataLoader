package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.MappingFileBuilder
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.fixtures.StudyInfo
import groovy.sql.Sql
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.Fixtures.*
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.hamcrest.core.IsNot.not
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertThat

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
        Study.deleteById(config, clinicalData.studyId)

        def result = clinicalData.load(config)

        expect:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(db, hasRecord('i2b2demodata.patient_dimension',
                ['sourcesystem_cd': "${studyId}:HCC827"], [age_in_years_num: 20]))
    }

    def 'it should produce SummaryStatistic.txt'() {
        when:
        Study.deleteById(config, clinicalData.studyId)

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
        Study.deleteById(config, conceptPath)
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
        def clinicalData = additionalStudiesDir.studyDir('Test Study With Non Unique Column Names', 'GSE0NQCN').clinicalData
        def successfullyLoaded = clinicalData.load(config + [allowNonUniqueColumnNames: true])
        then:
        noExceptionThrown()
        successfullyLoaded
    }

    def 'it should load category_cd and data_label with plus sign'() {
        when:
        def clinicalData = Fixtures.clinicalDataWithPlusSign
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

        assertThat db, hasRecord("i2b2", [c_fullname: "$timepointsPath\\Baseline\\"], [
                c_metadataxml: {
                    assertThat(it, notNullValue())

                    def metadata = new XmlParser().parseText(it as String)
                    assertThat(metadata.Oktousevalues.text(), equalTo('Y'))
                    assertThat(metadata.SeriesMeta.Value.text(), equalTo('0'))
                    assertThat(metadata.SeriesMeta.Unit.text(), equalTo('minutes'))
                    assertThat(metadata.SeriesMeta.DisplayName.text(), equalTo('Baseline'))
                    true
                }
        ])

        assertThat db, hasRecord("i2b2", [c_fullname: "$timepointsPath\\Month 2\\"], [
                c_metadataxml: {
                    def metadata = new XmlParser().parseText(it as String)
                    assertThat(metadata.Oktousevalues.text(), equalTo('Y'))
                    assertThat(metadata.SeriesMeta.Value.text(), equalTo((60 * 24 * 30 * 2).toString()))
                    assertThat(metadata.SeriesMeta.Unit.text(), equalTo('minutes'))
                    assertThat(metadata.SeriesMeta.DisplayName.text(), equalTo('Month 2'))
                    true
                }
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

        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '0', 'Baseline')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '5', '5 minutes')
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
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '60', '1 hour')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '120', '2 hours')
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
                    row '7',period3, 'One', '2000-12-31 12:00'
                    row '2', period2, 'One', '2000-12-31 12:00'
                    row '4', period4, 'One', '2000-12-31 12:00'
                }
                forSubject('SUBJ2') {
                    row '10', period1, 'Two', '2000-12-31 12:00'
                    row '9', period2, 'Two', '2000-12-31 12:00'
                    row '11', '2000-12-31 11:05', 'Two', '2000-12-31 12:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with timestamp\\Vars\\Timestamp"

        when:
        clinicalData.load(config)

        then:
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '-60', '-1 hour')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '-55', '-55 minutes')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '0', 'Baseline')
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
        def period2 = '2000-12-31 11:02'
        def period3 = '2000-12-31 12:03'
        def period4 = '2000-12-31 11:05'
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
                    row '7',period3, 'Male', '2000-12-31 12:00'
                    row '2', period2, 'Male', '2000-12-31 12:00'
                    row '4', period4, 'Male', '2000-12-31 12:00'
                }
                forSubject('SUBJ3') {
                    row '0', period1, 'Male', '2000-12-31 12:00'
                    row '1', period2, 'Male', '2000-12-31 12:00'
                    row '2', period4, 'Male', '2000-12-31 12:00'
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

        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '-55', '-55 minutes')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '3', '3 minutes')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '0', 'Baseline')

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

        def period1 = '2000-12-31 13:00'
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
                    row '5', period1, 'Male', '2000-12-31 12:00'
                    row '7',period3, 'Male', '2000-12-31 12:00'
                    row '2', period2, 'Male', '2000-12-31 12:00'
                    row '4', period4, 'Male', '2000-12-31 12:00'
                }
                forSubject('SUBJ3') {
                    row '0', period1, 'Male', '2000-12-31 12:00'
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

        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '60', '1 hour')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '65', '1 hour 5 minutes')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '0', 'Baseline')
    }

    def 'it should load Serial LDD data with timestamp with two timestamp column'() {
        given:
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

        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '0', 'Baseline')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '5', '5 minutes')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPathForSecond, '120', '2 hours')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPathForSecond, '123', '2 hours 3 minutes')
    }

    def 'it should load Serial LDD data with timestamp with two timestamp and baseline'() {
        given:
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
                    row '0', '2000-12-31 12:00', 'Female', '2000-12-31 12:00', '1', '2000-12-31 14:00','2000-12-31 13:00'
                    row '10', '2000-12-31 12:01', 'Female', '2000-12-31 12:00', '2', '2000-12-31 14:01','2000-12-31 13:00'
                    row '12', '2000-12-31 12:02', 'Female', '2000-12-31 12:00', '3', '2000-12-31 14:02','2000-12-31 13:00'
                    row '10', '2000-12-31 12:05', 'Female', '2000-12-31 12:00', '4', '2000-12-31 14:03','2000-12-31 13:00'
                }
                forSubject('SUBJ2') {
                    row '5', '2000-12-31 12:00', 'Male', '2000-12-31 12:00', '1', '2000-12-31 14:00','2000-12-31 13:00'
                    row '13', '2000-12-31 12:02', 'Male', '2000-12-31 12:00', '10', '2000-12-31 14:01','2000-12-31 13:00'
                    row '15', '2000-12-31 12:05', 'Male', '2000-12-31 12:00', '100', '2000-12-31 14:02','2000-12-31 13:00'
                }
            }
        }
        String timepointsPath = "\\Test Studies\\Test Study With Serial LDD with two timestamp and baseline\\Vars\\Timestamp"
        String timepointsPathForSecond = "\\Test Studies\\Test Study With Serial LDD with two timestamp and baseline\\Other\\Timestamp2"

        when:
        clinicalData.load(config)

        then:
        assertThat db, hasNode("$timepointsPath\\Baseline\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPath\\1 minute\\").withPatientCount(1)
        assertThat db, hasNode("$timepointsPathForSecond\\1 hour\\").withPatientCount(2)
        assertThat db, hasNode("$timepointsPathForSecond\\1 hour 3 minutes\\").withPatientCount(1)

        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '0', 'Baseline')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPath, '5', '5 minutes')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPathForSecond, '60', '1 hour')
        assertThat db, checkMetaDataXMLForTimestamp(timepointsPathForSecond, '63', '1 hour 3 minutes')
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
}