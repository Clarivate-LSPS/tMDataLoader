package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.ExpressionData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import org.junit.Assert
import spock.lang.Specification

import static org.hamcrest.CoreMatchers.equalTo
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertTrue

/**
 * Created by Alexander Omelchenko on 15.12.2015.
 */
class DataProcessorTest extends Specification implements ConfigAwareTestCase {

    private ClinicalDataProcessor _processor

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
    String studyExpressionName = expressionData.studyName
    String studyExpressionId = expressionData.studyId
    String platformId = 'GEX_TST'


    void setup() {
        ConfigAwareTestCase.super.setUp()
    }

//    ExpressionDataProcessor getExpProcessor() {
//        _processor ?: (_processor = new ExpressionDataProcessor(config))
//    }
//
//    ClinicalDataProcessor getProcessor() {
//        _processor ?: (_processor = new ClinicalDataProcessor(config))
//    }

    void 'Upload data with SECURITY'() {
        setup:
        cleanAll()

        config.securitySymbol = 'Y'
        def result = clinicalData.load(config)

        expect:
        assertThat("Clinical data loading shouldn't fail", result, equalTo(true))
        checkSetSecurityStatus(studyId, 1)

    }

    void 'Reupload by same path, different studyId without replace study option Clinical data'() {
        setup:
        cleanAll()

        config.securitySymbol = 'Y'
        clinicalData.load(config)

        expect:
        try{
            secondClinicalData.load(config)
        } catch(Exception e){
            assertEquals((String)"Other study with same path found by different studyId: ${originalPath}", e.getMessage())
            return
        }
        Assert.fail("Expected validation exception was not thrown");
    }

    void 'Reupload by same path, different studyId without replace study option Expression data'() {
        setup:
        cleanAll()

        expressionData.load(config)

        expect:
        try{
            secondExpressionData.load(config)
        } catch(Exception e){
            assertEquals((String)"Other study with same path found by different studyId: ${originalPath}", e.getMessage())
            return
        }
        Assert.fail("Expected validation exception was not thrown");
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
        assertTrue(checkSetSecurityStatus(studyId, 0))
        assertTrue(checkSetSecurityStatus(secondStudyId, 1))

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
        def tables = [
                ['table': 'biomart.bio_experiment', 'value': remStudyId, 'column': 'accession'],
                ['table': 'biomart.bio_data_uid', 'value': ("EXP:" + remStudyId).toUpperCase(), 'column': 'unique_id'],
                ['table': 'searchapp.search_secure_object', 'value': ("EXP:" + remStudyId).toUpperCase(), 'column': 'bio_data_unique_id']
        ]
        for (t in tables) {
            def res = sql.execute("DELETE FROM ${t.table} WHERE ${t.column} = ?",
                    [t.value])
        }

    }

    private Boolean checkSetSecurityStatus(checkStudyId, value) {

        def tables = [
                ['table': 'biomart.bio_experiment', 'value': checkStudyId, 'column': 'accession'],
                ['table': 'biomart.bio_data_uid', 'value': ("EXP:" + checkStudyId).toUpperCase(), 'column': 'unique_id'],
                ['table': 'searchapp.search_secure_object', 'value': ("EXP:" + checkStudyId).toUpperCase(), 'column': 'bio_data_unique_id']
        ]
        for (t in tables) {
            def res = sql.firstRow("SELECT COUNT(*) AS cnt FROM ${t.table} WHERE ${t.column} = ?",
                    [t.value])
            if (res.cnt != value) return false
        }

        return true
    }

    void 'Upload study with clinical data into top directory'(){
        setup:
        cleanAll()

        clinicalData.load(config, "${originalPath}")
        fourthClinicalData.load(config,"${originalPath}")


        expect:
        try{
            thirdClinicalData.loadByPath(config, originalPath)
        } catch(Exception e){
            assertEquals((String)"This path contains several different studyId : ${originalPath}", e.getMessage())
            return
        }
        Assert.fail("Expected validation exception was not thrown");
    }

}
