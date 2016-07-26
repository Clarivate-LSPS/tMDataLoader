package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.files.MetaInfoHeader
import com.thomsonreuters.lsps.utils.DbUtils
import groovy.io.FileType
import groovy.sql.Sql
import org.anarres.lzo.LzoAlgorithm
import org.anarres.lzo.LzoCompressor
import org.anarres.lzo.LzoLibrary
import org.anarres.lzo.LzoOutputStream

import java.nio.file.Files
import java.nio.file.Path

/**
 * Date: 21-Apr-16
 * Time: 15:52
 */
class GWASPlinkDataProcessor extends AbstractDataProcessor {
    GWASPlinkDataProcessor(config) {
        super(config)
    }

    private static String[] validateFamAndCollectPatientID(Path fam) {
        LinkedList<String> patientIds = new LinkedList<String>()
        fam.eachLine { String line, Integer lineNum ->
            def prefix = "${fam.fileName.toString()}:$lineNum"
            line = line.trim()
            if (line.isEmpty()) return

            def tokens = line.split("\\s+")
            if (tokens.length != 6)
                throw new DataProcessingException("$prefix: Invalid columns count: expected 6, but was $tokens.length")

            def withinFamilyId = tokens[1]
            if (withinFamilyId == '0') {
                throw new DataProcessingException("$prefix: Invalid IID, cannot be '0'")
            }

            def sex = tokens[4]
            if (!(sex in ['0', '1', '2'])) {
                throw new DataProcessingException("$prefix: Invalid sex value. Expected '1' = male, '2' = female, '0' = unknown, but was '${sex}'")
            }

            patientIds.add(tokens[1])
        }
        return patientIds
    }

    private static Path detectFile(Path dir, String ext) {
        List<Path> paths = []
        dir.eachFileMatch(FileType.FILES, { String it -> it.endsWith(ext) }, paths.&add)
        if (paths.size() > 1) {
            throw new DataProcessingException("Can't detect $ext: too many candidates (${paths*.fileName.join(', ')})")
        } else if (!paths) {
            throw new DataProcessingException("Can't detect $ext: no candidates")
        }
        paths[0]
    }

    @Override
    boolean processFiles(Path dir, Sql sql, Object studyInfo) {
        def mappingFilePath = null
        dir.eachFileMatch(FileType.FILES, { it ==~ /(?:^|_)MappingFile\.txt$/ }) {
            mappingFilePath = it
        }
        if (!mappingFilePath)
            throw new DataProcessingException("Missing mapping file for GWAS Plike Data")

        def metaInfo = MetaInfoHeader.getMetaInfo(mappingFilePath, "# ")
        String studyId = metaInfo.STUDY_ID
        if (!studyId) {
            throw new DataProcessingException("No STUDY_ID specified in mapping file (ex: # STUDY_ID: MYSTUDY)")
        }
        Path bed, bim, fam
        if (metaInfo.BFILE) {
            String bfile = metaInfo.BFILE
            bed = dir.resolve("${bfile}.bed")
            bim = dir.resolve("${bfile}.bim")
            fam = dir.resolve("${bfile}.fam")
        } else {
            bed = detectFile(dir, '.bed')
            bim = detectFile(dir, '.bim')
            fam = detectFile(dir, '.fam')
        }
        studyInfo['id'] = studyId
        def missingFiles = [bed, bim, fam].findAll { Files.notExists(it) }
        if (missingFiles) {
            throw new DataProcessingException("One or more required files are missing: ${missingFiles*.fileName.join(', ')}")
        }

        LinkedList<String> patientIds = validateFamAndCollectPatientID(fam)

        sql.execute('delete from gwas_plink.plink_data where study_id = ?', studyId)
        DbUtils.insertRecord(database, sql, 'gwas_plink.plink_data', [study_id: studyId],
                [
                        bed: compressedStream(bed),
                        bim: compressedStream(bim),
                        fam: compressedStream(fam)
                ])

        SamplesLoader samplesLoader = new SamplesLoader(studyId)
        String categoryCd = metaInfo.CATEGORY_CD ?: 'GWAS+GWAS Plink'
        patientIds.each {
            samplesLoader.addSample(categoryCd, it, it, '')
        }
        samplesLoader.loadSamples(database, sql)
        return true
    }

    @Override
    public boolean runStoredProcedures(jobId, Sql sql, studyInfo) {
        def studyId = studyInfo['id']
        def studyNode = studyInfo['node']
        if (studyId && studyNode) {
            config.logger.log("Study ID=${studyId}; Node=${studyNode}")
            sql.call("{call ${config.controlSchema}.$procedureName(?,?,?,?)}", [studyId, studyNode, config.securitySymbol, jobId])
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
            return false;
        }
        return true;
    }

    @Override
    public String getProcedureName() {
        return "I2B2_PROCESS_GWAS_PLINK_DATA"
    }

    public static InputStream compressedStream(final Path input) throws IOException {
        final LzoAlgorithm algorithm = LzoAlgorithm.LZO1X;
        final LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(algorithm, null);
        final PipedInputStream inputStream = new PipedInputStream()
        final PipedOutputStream outputStream = new PipedOutputStream(inputStream)
        Thread.start {
            def stream = new LzoOutputStream(outputStream, compressor)
            stream.withCloseable {
                input.withInputStream { stream << it }
            }
        }
        return inputStream
    }
}
