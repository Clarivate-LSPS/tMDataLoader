package com.thomsonreuters.lsps.transmart

import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.ExpressionData
import com.thomsonreuters.lsps.transmart.fixtures.GWASPlinkData
import com.thomsonreuters.lsps.transmart.fixtures.ProteinData
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

        ProteinData getProteinData(String proteinDataFolder = 'ProteinDataToUpload') {
            return new ProteinData(studyInfo: studyInfo, dir: new File(this, proteinDataFolder))
        }

        GWASPlinkData getGWASPlinkData(String gwasPlinkDataFolder = 'GWASPlinkDataToUpload') {
            return new GWASPlinkData(studyInfo: studyInfo, dir: new File(this, gwasPlinkDataFolder))
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

    static def getStudiesForMerge() {
        return [update : new StudiesDirFile(['fixtures', 'Test Studies For Merge', 'Update'].join(File.separator)),
                append : new StudiesDirFile(['fixtures', 'Test Studies For Merge', 'Append'].join(File.separator)),
                replace : new StudiesDirFile(['fixtures', 'Test Studies For Merge', 'Replace'].join(File.separator)),
                update_var: new StudiesDirFile(['fixtures', 'Test Studies For Merge', 'UpdateVariables'].join(File.separator)),
                first_load:  new StudiesDirFile(['fixtures', 'Test Studies For Merge', 'First Load'].join(File.separator))]
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

    static ClinicalData getClinicalDataWithPlusSign() {
        return studiesDir.studyDir('Test Study With Plus Sign', 'GSE0PLS').clinicalData
    }

    static ClinicalData getClinicalDataWithLongCategoryCD() {
        return studiesDir.studyDir('Test Study With Long CategoryCD', 'GSE0LONGCCD').clinicalData
    }

    static ClinicalData getClinicalDataWithWrongMappingFileName() {
        return studiesDir.studyDir('Test Study With Wrong Mapping File_Name', 'GSE0WRONGMAPF').clinicalData
    }

    static ClinicalData getClinicalDataWithNonVisialSymbols() {
        return studiesDir.studyDir('Test Study With Non Visial Symbols In Header', 'GSE0NOVIS').clinicalData
    }

    static ClinicalData getClinicalDataWithDifferentStudyID() {
        return studiesDir.studyDir('Test Study With Different StudyID', 'GSE0DIFFSID').clinicalData
    }

    static ClinicalData getClinicalDataWithDifferentStudyIDVar2() {
        return studiesDir.studyDir('Test Study With Different StudyID In Two Files', 'GSE0DIFFSIDVAR2').clinicalData
    }

    static ClinicalData getClinicalDataWithTerminator() {
        return studiesDir.studyDir('Test Study With Terminator', 'GSE0TERMINATOR').clinicalData
    }

    static ClinicalData getClinicalDataWithTerminatorAndSamePath() {
        return studiesDir.studyDir('Test Study With Repeating Labels In Path', 'GSE0REPEATLABPATH').clinicalData
    }

    static ClinicalData getClinicalDataWithDataValueInPath() {
        return studiesDir.studyDir('Test Study With Data Value In Path', 'GSE0DVINPATH').clinicalData
    }

    static ClinicalData getClinicalDataForCaseSensitive() {
        return studiesDir.studyDir('Test Study', 'GSE0LETTER').clinicalData
    }

    static ClinicalData getClinicalDataWithSingleVisitName() {
        return studiesDir.studyDir('Test Study With Single Visit Name', 'GSE0SINGLEVN').clinicalData
    }

    static ClinicalData getClinicalDataWithDuplicatedPatientId() {
        return studiesDir.studyDir('Test Study With Duplicated Patient ID', 'GSE0DUPPID').clinicalData
    }

    static ClinicalData getClinicalDataWithExtraLevel() {
        return studiesDir.studyDir('Test Study Deeper in the Tree', 'GSE0EL').clinicalData
    }

    static ProteinData getProteinData() {
        return studiesDir.studyDir('Test Protein Study', 'GSE37425').proteinData
    }

    static ProteinData getAdditionalProteinData() {
        return studiesDir.studyDir('Test Protein Study', 'GSE37425').getProteinData('ProteinDataToUpload (Additional)')
    }

    static ProteinData getProteinDataWithoutPeptide() {
        return studiesDir.studyDir('Test Protein Study 2', 'GSE374251').proteinData
    }

    static ProteinData getProteinDataWithoutPeptide3() {
        return studiesDir.studyDir('Test Protein Study 3', 'GSE374253').proteinData
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
