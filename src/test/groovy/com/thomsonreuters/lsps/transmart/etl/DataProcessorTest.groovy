package com.thomsonreuters.lsps.transmart.etl
import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.ExpressionData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

import static org.junit.Assert.assertEquals
/**
 * Created by Alexander Omelchenko on 15.12.2015.
 */
class DataProcessorTest extends Specification implements ConfigAwareTestCase {
    ClinicalData clinicalData = Fixtures.clinicalData
    ClinicalData secondClinicalData = clinicalData.copyAttachedToStudy(clinicalData.studyInfo.withSuffixForId("_2"))
    ClinicalData thirdClinicalData = clinicalData.copyWithSuffix('THD')
    ClinicalData fourthClinicalData = clinicalData.copyWithSuffix('FTH')

    String rootName = 'Test Studies'
    String studyName = clinicalData.studyName
    String studyId = clinicalData.studyId
    String secondStudyId = secondClinicalData.studyId
    String thirdClinicalDataStudyId = thirdClinicalData.studyId
    String fourthClinicalDataStudyId = fourthClinicalData.studyId
    String originalPath = "\\$rootName\\$studyName\\"

    ExpressionData expressionData = Fixtures.getExpressionData()
    ExpressionData secondExpressionData = expressionData.copyAttachedToStudy(expressionData.studyInfo.withSuffixForId("_2"))

    void setup() {
        ConfigAwareTestCase.super.setUp()
    }

    def 'it should upload data with SECURITY'() {
        setup:
        cleanAll()

        config.securitySymbol = 'Y'

        when:
        def loadedSuccessfully = clinicalData.load(config)

        then:
        loadedSuccessfully
        checkSetSecurityStatus(studyId, 1)
    }

    def "it shouldn't load study if study with same id already loaded by different path"() {
        setup:
        clinicalData.reload(config)

        when:
        def sameStudyByDifferentPath = clinicalData.studyInfo.withName('Other Test Study Path')
        clinicalData.copyAttachedToStudy(sameStudyByDifferentPath).load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "Other study with same id found by different path: \\Test Studies\\Test Study\\" as String
    }

    void 'Reupload by same path, different studyId without replace study option Clinical data'() {
        setup:
        cleanAll()

        config.securitySymbol = 'Y'
        clinicalData.load(config)

        when:
        secondClinicalData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "Other study by same path found with different studyId: ${originalPath}" as String
    }

    void 'Reupload by same path, different studyId without replace study option Expression data'() {
        setup:
        cleanAll()

        expressionData.load(config)

        when:
        secondExpressionData.load(config)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "Other study by same path found with different studyId: ${originalPath}" as String
    }

    void 'Reupload by same path, different studyId with replace study option'() {
        setup:
        cleanAll()

        config.securitySymbol = 'Y'
        config.replaceStudy = true
        clinicalData.load(config)
        def bioExpId = sql.firstRow("select bio_experiment_id as bioExpId from biomart.bio_experiment where accession = ?", [studyId])
        def bioDataId = sql.firstRow("select bio_data_id as bioDataId from biomart.bio_data_uid where unique_id = ?",
                [("EXP:" + studyId).toUpperCase()])
        def searchSecObjId = sql.firstRow("select search_secure_object_id as searchSecObjId from searchapp.search_secure_object where bio_data_unique_id = ?",
                [("EXP:" + studyId).toUpperCase()])
        secondClinicalData.load(config)

        expect:
        checkSetSecurityStatus(studyId, 0)
        checkSetSecurityStatus(secondStudyId, 1)

        def bioExpIdNew = sql.firstRow("select bio_experiment_id as bioExpId from biomart.bio_experiment where accession = ?", secondStudyId)
        def bioDataIdNew = sql.firstRow("select bio_data_id as bioDataId from biomart.bio_data_uid where unique_id = ?",
                [("EXP:" + secondStudyId).toUpperCase()])
        def searchSecObjIdNew = sql.firstRow("select search_secure_object_id as searchSecObjId from searchapp.search_secure_object where bio_data_unique_id = ?",
                [("EXP:" + secondStudyId).toUpperCase()])
        assertEquals(bioExpId.bioExpId, bioExpIdNew.bioExpId)
        assertEquals(bioDataId.bioDataId, bioDataIdNew.bioDataId)
        assertEquals(searchSecObjId.searchSecObjId, searchSecObjIdNew.searchSecObjId)
    }

    private void cleanAll() {
        Study.deleteById(config, studyId)
        Study.deleteById(config, secondStudyId)
        Study.deleteById(config, thirdClinicalDataStudyId)
        Study.deleteById(config, fourthClinicalDataStudyId)

        cleanOldData(studyId)
        cleanOldData(secondStudyId)
    }

    private void cleanOldData(remStudyId) {
        remStudyId = remStudyId.toUpperCase()
        def tables = [
                ['table': 'biomart.bio_experiment', 'value': remStudyId, 'column': 'accession'],
                ['table': 'biomart.bio_data_uid', 'value': "EXP:$remStudyId", 'column': 'unique_id'],
                ['table': 'searchapp.search_secure_object', 'value': "EXP:$remStudyId", 'column': 'bio_data_unique_id']
        ]
        for (t in tables) {
            sql.execute("DELETE FROM ${t.table} WHERE ${t.column} = ?", [t.value as String])
        }

    }

    private void checkSetSecurityStatus(String checkStudyId, value) {
        checkStudyId = checkStudyId.toUpperCase()
        def tables = [
                ['table': 'biomart.bio_experiment', 'value': checkStudyId, 'column': 'accession'],
                ['table': 'biomart.bio_data_uid', 'value': "EXP:$checkStudyId", 'column': 'unique_id'],
                ['table': 'searchapp.search_secure_object', 'value': "EXP:$checkStudyId", 'column': 'bio_data_unique_id']
        ]
        for (t in tables) {
            def res = sql.firstRow("SELECT COUNT(*) AS cnt FROM ${t.table} WHERE ${t.column} = ?", [t.value as String])
            assert res.cnt == value, "Number of records in ${t.table} (${res.cnt}) doesn't match expected ($value)"
        }
    }

    void 'Upload study with clinical data into top directory'(){
        setup:
        cleanAll()

        clinicalData.load(config, "${originalPath}")
        fourthClinicalData.load(config,"${originalPath}")

        when:
        thirdClinicalData.loadByPath(config, originalPath)

        then:
        def ex = thrown(DataProcessingException)
        ex.message == "'\\Test Studies\\Test Study\\' path contains several different studyIds: [GSE0, GSE0FTH]"
    }

}
