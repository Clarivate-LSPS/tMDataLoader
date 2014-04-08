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
        loadMetadata(sql, studyInfo)
        def samplesLoader = new SamplesLoader(studyInfo.id)
        dir.eachFileMatch(FileType.FILES, ~/(?i).*\.vcf$/) {
            processFile(it, sql, samplesLoader, studyInfo)
        }
        samplesLoader.loadSamples(sql)
        return true
    }

    def loadMetadata(Sql sql, studyInfo) {
        use(SqlMethods) {
            logger.log(LogType.DEBUG, 'Loading study information into deapp.de_variant_dataset')
            sql.insertRecord('deapp.de_variant_dataset',
                    dataset_id: studyInfo.id, etl_id: 'tMDataLoader', genome: 'hg19',
                    etl_date: Calendar.getInstance())
        }
    }

    def processFile(File inputFile, Sql sql, SamplesLoader samplesLoader, studyInfo) {
        def vcfFile = new VcfFile(inputFile)
        def sampleMapping = studyInfo.sampleMapping
        String trialId = studyInfo.id
        logger.log(LogType.MESSAGE, "Processing file ${inputFile.getName()}")
        SqlMethods.insertRecords(sql, 'deapp.de_variant_subject_idx', ['dataset_id', 'subject_id', 'position']) { st ->
            vcfFile.samples.eachWithIndex { sample, idx ->
                logger.log(LogType.DEBUG, 'Loading samples')
                st.addBatch([trialId, sample, idx + 1])
                samplesLoader.addSample("VCF+${inputFile.name.replaceFirst(/\.\w+$/, '')}", sampleMapping[sample] as String, sample, '')
            }
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
