package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.files.MetaInfoHeader
import com.thomsonreuters.lsps.transmart.files.VcfFile
import com.thomsonreuters.lsps.transmart.sql.SqlMethods
import groovy.io.FileType
import groovy.sql.Sql

/**
 * Created by bondarev on 4/3/14.
 */
class VCFDataProcessor extends DataProcessor {
    VCFDataProcessor(Object conf) {
        super(conf)
    }

    private void loadMappingFile(File mappingFile, studyInfo) {
        def csv = new CsvLikeFile(mappingFile, '#')
        if (!studyInfo.id) {
            use(MetaInfoHeader) {
                studyInfo.id = csv.metaInfo.STUDY_ID
            }
        }
        def sampleMapping = [:]
        csv.eachEntry {
            String subjectId = it[0]
            String sampleCd = it[1]
            sampleMapping[sampleCd] = subjectId
        }
        studyInfo.sampleMapping = sampleMapping
    }

    @Override
    boolean processFiles(File dir, Sql sql, studyInfo) {
        File mappingFile = new File(dir, 'Subject_Sample_Mapping_File.txt')
        if (!mappingFile.exists()) {
            logger.log(LogType.ERROR, "Mapping file not found")
            return false
        }
        loadMappingFile(mappingFile, studyInfo)
        def samplesLoader = new SamplesLoader(studyInfo.id)
        dir.eachFileMatch(FileType.FILES, ~/(?i).*\.vcf$/) {
            processFile(it, samplesLoader, studyInfo)
        }
        samplesLoader.loadSamples(sql)
        return true
    }

    def processFile(File inputFile, SamplesLoader samplesLoader, studyInfo) {
        def vcfFile = new VcfFile(inputFile)
        def sampleMapping = studyInfo.sampleMapping
        vcfFile.samples.each { sample ->
            samplesLoader.addSample("VCF+${inputFile.name.replaceFirst(/\.\w+$/, '')}", sampleMapping[sample] as String, sample, '')
        }
    }

    @Override
    boolean runStoredProcedures(Object jobId, Sql sql, Object studyInfo) {
        def studyId = studyInfo['id']
        def studyNode = studyInfo['node']
        if (studyId && studyNode) {
            use(SqlMethods) {
                sql.callProcedure("${config.controlSchema}.i2b2_process_vcf_data",
                        studyId, studyNode, 'STD', config.securitySymbol, jobId)
            }
            return true
        } else {
            logger.log(LogType.ERROR, 'Study ID or Node not defined')
            return false
        }
    }

    @Override
    String getProcedureName() {
        return "${config.controlSchema}.I2B2_PROCESS_VCF_DATA"
    }
}
