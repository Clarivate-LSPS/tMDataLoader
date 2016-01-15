package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.MappingFileBuilder
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.fixtures.StudyInfo
import groovy.sql.Sql
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.Fixtures.getAdditionalStudiesDir
import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.core.IsNot.not
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
        def result = clinicalData.load(config)

        expect:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        assertThat(db, hasRecord('i2b2demodata.patient_dimension',
                ['sourcesystem_cd': "${studyId}:HCC827"], [age_in_years_num: 20]))
    }

    def 'it should produce SummaryStatistic.txt'() {
        when:
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
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"

        processor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient('HCC2935').inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M\\'))
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

        assertThat(sql, hasNode("$demoPath\\Age (AGE)\\").withPatientCount(9))
        assertThat(sql, not(hasNode("$demoPath\\Age (AGE)\\Baseline\\")))

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

        assertThat(sql, hasNode("$demoPath\\Age (AGE)\\Baseline\\").withPatientCount(9))

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
        Study.deleteByPath(config, "\\Test Studies\\${studyName}")
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
        thrown(DataProcessingException)
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
}