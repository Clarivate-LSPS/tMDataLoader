package com.thomsonreuters.lsps.transmart

import org.codehaus.groovy.runtime.DefaultGroovyMethods

/**
 * Created by bondarev on 4/3/14.
 */
class Fixtures {
    static class ExpressionData {
        String studyId
        File path
    }

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

    static ExpressionData getExpressionData(String studyName = 'Test Study', String studyId = 'GSE0') {
        new ExpressionData(studyId: studyId, path: new File(studyDir(studyName, studyId), 'ExpressionDataToUpload'))
    }

    static File getClinicalData(String studyName = 'Test Study', String studyId = 'GSE0') {
        new File(studyDir(studyName, studyId), 'ClinicalDataToUpload')
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
