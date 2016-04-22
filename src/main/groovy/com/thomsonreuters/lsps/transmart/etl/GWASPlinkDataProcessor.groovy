package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.io.file.PathUtils
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.files.MetaInfoHeader
import groovy.io.FileType

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Blob

/**
 * Date: 21-Apr-16
 * Time: 15:52
 */
class GWASPlinkDataProcessor implements DataProcessor {
    def config
    Database database

    GWASPlinkDataProcessor(config) {
        this.config = config
        database = TransmartDatabaseFactory.newDatabase(config)
    }

    private String detectBfile(Path dir) {
        def bedPaths = dir.findAll { p -> p.fileName.toString().endsWith('.bed') }
        if (bedPaths.size() > 1) {
            throw new DataProcessingException("Can't detect BFILE: too many candidates (${bedPaths*.fileName.join(', ')})")
        } else if (!bedPaths) {
            throw new DataProcessingException("Can't detect BFILE: no candidates")
        }
        bedPaths[0].fileName.toString().replaceFirst(/\.bed$/, '')
    }

    @Override
    boolean process(Path dir, Object studyInfo) {
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
        String bfile = metaInfo.BFILE ?: detectBfile(dir)
        Path bem = dir.resolve("${bfile}.bed")
        Path bim = dir.resolve("${bfile}.bim")
        Path fam = dir.resolve("${bfile}.fam")
        def missingFiles = [bem, bim, fam].findAll { Files.notExists(it) }
        if (missingFiles) {
            throw new DataProcessingException("One or more required files are missing: ${missingFiles*.fileName.join(', ')}")
        }

        database.withSql { sql ->
            sql.execute('delete from gwas_plink.plink_data where study_id = ?', studyId)
            sql.execute("insert into gwas_plink.plink_data (study_id, bed, bim, fam) values (?, ?, ?, ?)",
                    studyId, bem.bytes, bim.bytes, fam.bytes)
        }
        return true
    }
}
