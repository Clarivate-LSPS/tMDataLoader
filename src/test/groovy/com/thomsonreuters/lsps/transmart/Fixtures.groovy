package com.thomsonreuters.lsps.transmart

import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.ExpressionData
import com.thomsonreuters.lsps.transmart.fixtures.StudyInfo
import com.thomsonreuters.lsps.transmart.fixtures.VCFData
import org.codehaus.groovy.runtime.DefaultGroovyMethods

/**
 * Created by bondarev on 4/3/14.
 */
class Fixtures {
    static class StudyDirFile extends File {
        StudyInfo studyInfo

        StudyDirFile(File parent, String studyId, String studyName) {
            super(parent, "${studyName}_${studyId}")
            this.studyInfo = new StudyInfo(studyId, studyName)
        }

        ExpressionData getExpressionData(String expressionDataFolder = 'ExpressionDataToUpload') {
            return new ExpressionData(studyInfo: studyInfo, dir: new File(this, expressionDataFolder))
        }

        ClinicalData getClinicalData(String clinicalDataFolder = 'ClinicalDataToUpload') {
            return new ClinicalData(studyInfo: studyInfo, dir: new File(this, clinicalDataFolder))
        }

        VCFData getVCFData(String vcfDataFolder = 'VCFDataToUpload') {
            return new VCFData(studyInfo: studyInfo, dir: new File(this, vcfDataFolder))
        }
    }

    static class StudiesDirFile extends File {
        StudiesDirFile(String path) {
            super(path)
        }

        StudyDirFile studyDir(String studyName, String studyId) {
            return new StudyDirFile(this, studyId, studyName)
        }
    }

    static StudiesDirFile getStudiesDir() {
        return new StudiesDirFile(['fixtures', 'Test Studies'].join(File.separator))
    }

    static StudiesDirFile getAdditionalStudiesDir() {
        return new StudiesDirFile(['fixtures', 'Additional Test Studies'].join(File.separator))
    }

    static StudyDirFile studyDir(String studyName, String studyId, File studiesDir = Fixtures.studiesDir) {
        return new StudyDirFile(studiesDir, studyId, studyName)
    }

    static VCFData getVcfData(String studyName = 'Test Study', String studyId = 'GSE0') {
        return studyDir(studyName, studyId).getVCFData()
    }

    static VCFData getMultipleVcfData(String studyName = 'Test Study', String studyId = 'GSE0') {
        return studyDir(studyName, studyId).getVCFData('MultiVCFDataToUpload')
    }

    static VCFData getMultipleVcfDataWithConfiguredPaths() {
        return studyDir('Test Study', 'GSE0').getVCFData('MultiVCFDataWithConfiguredPaths')
    }

    static ExpressionData getExpressionData(String studyName = 'Test Study', String studyId = 'GSE0') {
        return studyDir(studyName, studyId).expressionData
    }

    static ClinicalData getClinicalData(String studyName = 'Test Study', String studyId = 'GSE0') {
        return studiesDir.studyDir(studyName, studyId).clinicalData
    }

    public static class FilePathBuilder {
        File file

        public FilePathBuilder(File file) {
            this.file = file
        }

        public <T> T asType(Class<T> cls) {
            if (cls.isAssignableFrom(File.class)) {
                return cls.cast(file);
            } else {
                return DefaultGroovyMethods.asType(cls);
            }
        }

        @Override
        Object getProperty(String property) {
            File file = new File(file, property);
            if (!file.exists()) {
                throw new IllegalArgumentException("Fixture not found at path: ${file.path}")
            }
            return new FilePathBuilder(file)
        }
    }

    static FilePathBuilder getInvalidStudies() {
        return new FilePathBuilder(new File("fixtures", "Invalid Studies"))
    }
}
