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

    private void cleanup(Sql sql, String studyId) {
        sql.execute('delete from deapp.de_variant_population_data where dataset_id = ?', studyId)
        sql.execute('delete from deapp.de_variant_population_info where dataset_id = ?', studyId)
        sql.execute('delete from deapp.de_variant_subject_summary where dataset_id = ?', studyId)
        sql.execute('delete from deapp.de_variant_subject_detail where dataset_id = ?', studyId)
        sql.execute('delete from deapp.de_variant_subject_idx where dataset_id = ?', studyId)
        sql.execute('delete from deapp.de_variant_dataset where dataset_id = ?', studyId)
        sql.commit()
    }

    @Override
    boolean processFiles(File dir, Sql sql, studyInfo) {
        File mappingFile = new File(dir, 'Subject_Sample_Mapping_File.txt')
        if (!mappingFile.exists()) {
            logger.log(LogType.ERROR, "Mapping file not found")
            return false
        }
        loadMappingFile(mappingFile, studyInfo)

        String studyId = studyInfo.id as String
        cleanup(sql, studyId)

        loadMetadata(sql, studyInfo)

        def samplesLoader = new SamplesLoader(studyId)
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
            sql.commit()
        }
    }

    def processFile(File inputFile, Sql sql, SamplesLoader samplesLoader, studyInfo) {
        def vcfFile = new VcfFile(inputFile)
        def sampleMapping = studyInfo.sampleMapping
        String trialId = studyInfo.id
        logger.log(LogType.MESSAGE, "Processing file ${inputFile.getName()}")
        use(SqlMethods) {
            DataLoader.start(database, 'deapp.de_variant_subject_idx', ['DATASET_ID', 'SUBJECT_ID', 'POSITION']) { st ->
                vcfFile.samples.eachWithIndex { sample, idx ->
                    logger.log(LogType.DEBUG, 'Loading samples')
                    st.addBatch([trialId, sample, idx + 1])
                    samplesLoader.addSample("VCF+${inputFile.name.replaceFirst(/\.\w+$/, '')}", sampleMapping[sample] as String, sample, '')
                }
            }

            logger.log(LogType.DEBUG, 'Loading population info')
            DataLoader.start(database, 'deapp.de_variant_population_info',
                    ['DATASET_ID', 'INFO_NAME', 'DESCRIPTION', 'type', 'number']) { populationInfo ->
                vcfFile.infoFields.values().each {
                    populationInfo.addBatch([trialId, it.id, it.description, it.type, it.number])
                }
            }

            logger.log(LogType.DEBUG, 'Loading subject summary, subject detail & population data')
            DataLoader.start(database, 'deapp.de_variant_subject_detail',
                    ['DATASET_ID', 'RS_ID', 'CHR', 'POS', 'REF', 'ALT', 'QUAL',
                     'FILTER', 'INFO', 'FORMAT', 'VARIANT_VALUE']) { subjectDetail ->
                DataLoader.start(database, 'deapp.de_variant_subject_summary',
                        ['DATASET_ID', 'SUBJECT_ID', 'RS_ID', 'CHR', 'POS', 'VARIANT', 'VARIANT_FORMAT', 'VARIANT_TYPE',
                         'REFERENCE', 'ALLELE1', 'ALLELE2']) { subjectSummary ->
                    DataLoader.start(database, 'deapp.de_variant_population_data',
                            ['DATASET_ID', 'CHR', 'POS', 'INFO_NAME', 'INFO_INDEX',
                             'INTEGER_VALUE', 'FLOAT_VALUE', 'TEXT_VALUE']) { populationData ->
                        vcfFile.eachEntry { VcfFile.Entry entry ->
                            writeVariantSubjectDetailRecord(trialId, subjectDetail, entry)
                            writeVariantSubjectSummaryRecords(trialId, subjectSummary, entry)
                            writeVariantPopulationDataRecord(trialId, populationData, entry)
                        }
                    }
                }
            }
        }
    }

    private void writeVariantPopulationDataRecord(String trialId, st, VcfFile.Entry entry) {
        entry.infoData.entrySet().each {
            VcfFile.InfoField infoField = it.key
            Object[] values = it.value
            String type = infoField.type.toLowerCase()
            Integer intValue
            Float floatValue
            String textValue
            values.eachWithIndex { value, int idx ->
                switch (type) {
                    case 'integer':
                    case 'flag':
                        intValue = value as int
                        break
                    case 'float':
                        floatValue = value as float
                        break
                    case 'character':
                    case 'string':
                        textValue = value as String
                        break
                }
                st.addBatch([trialId, entry.chromosome, entry.chromosomePosition, infoField.id,
                             idx, intValue, floatValue, textValue])
            }
        }
    }

    private void writeVariantSubjectSummaryRecords(String trialId, st, VcfFile.Entry entry) {
        CharSequence variantType = entry.reference.size() == 1 &&
                entry.alternatives.size() == 1 && entry.alternatives[0].size() == 1 ? 'SNV' : 'DIV'
        entry.samplesData.entrySet().each { sampleEntry ->
            VcfFile.SampleData sampleData = sampleEntry.value
            CharSequence variant = ''
            CharSequence variantFormat = ''
            Integer allele1 = sampleData.allele1 != '.' ? sampleData.allele1 as int : null
            Integer allele2 = sampleData.allele2 != '.' ? sampleData.allele2 as int : null
            if (sampleData.allele1 == '0') {
                variant += entry.reference
                variantFormat += 'R'
            } else {
                if (!allele1.is(null)) {
                    variant += entry.alternatives[allele1 - 1]
                    variantFormat += 'V'
                }
            }
            variant += sampleData.alleleSeparator
            variantFormat += sampleData.alleleSeparator
            if (sampleData.allele2 == '0') {
                variant += entry.reference
                variantFormat += 'R'
            } else {
                if (!allele2.is(null)) {
                    variant += entry.alternatives[allele2 - 1]
                    variantFormat += 'V'
                }
            }
            boolean reference = (allele1.is(null) || allele1 == 0) && (allele2.is(null) || allele2 == 0)
            st.addBatch([trialId, sampleEntry.key, entry.probesetId, entry.chromosome, entry.chromosomePosition,
                         variant, variantFormat, variantType,
                         reference, allele1, allele2
            ])
        }
    }

    private def writeVariantSubjectDetailRecord(CharSequence trialId, def st, VcfFile.Entry entry) {
        st.addBatch([
                trialId, entry.probesetId, entry.chromosome, entry.chromosomePosition, entry.reference,
                entry.alternatives.join(','), entry.qual, entry.filter, entry.infoString, entry.formatString,
                entry.sampleValues.join('\t')
        ])
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
