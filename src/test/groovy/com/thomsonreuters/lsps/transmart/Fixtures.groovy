package com.thomsonreuters.lsps.transmart

/**
 * Created by bondarev on 4/3/14.
 */
class Fixtures {
    static File getStudiesDir() {
        return new File(['fixtures', 'Test Studies'].join(File.separator))
    }

    static File getAdditionalStudiesDir() {
        return new File(['fixtures', 'Additional Test Studies'].join(File.separator))
    }

    static File studyDir(String studyName, String studyId, File studiesDir = Fixtures.studiesDir) {
        return new File(studiesDir, "${studyName}_${studyId}")
    }

    static File getVcfData(String studyName = 'Test Study', String studyId = 'GSE0') {
        new File(studyDir(studyName, studyId), 'VCFDataToUpload')
    }

    static File getMultipleVcfData(String studyName = 'Test Study', String studyId = 'GSE0') {
        new File(studyDir(studyName, studyId), 'MultiVCFDataToUpload')
    }

    static File getExpressionData(String studyName = 'Test Study', String studyId = 'GSE0') {
        new File(studyDir(studyName, studyId), 'ExpressionDataToUpload')
    }

    static File getClinicalData(String studyName = 'Test Study', String studyId = 'GSE0') {
        new File(studyDir(studyName, studyId), 'ClinicalDataToUpload')
    }
}
