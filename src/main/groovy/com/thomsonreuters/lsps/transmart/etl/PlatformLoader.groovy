package com.thomsonreuters.lsps.transmart.etl
import com.thomsonreuters.lsps.transmart.etl.platforms.GenePlatform
import groovy.sql.Sql
/**
 * Created by bondarev on 3/28/14.
 */
class PlatformLoader {
    Sql sql
    def config

    PlatformLoader(Sql sql, config) {
        this.sql = sql
        this.config = config
    }

    void doLoad(GenePlatform genePlatform, studyInfo) {
        genePlatform.cleanupTempTables(sql)
        if (!genePlatform.isLoaded(sql)) {
            // platform is not defined, loading
            config.logger.log("Loading platform: ${genePlatform.id}")
            if (!genePlatform.file.exists()) throw new Exception("Platform file not found: ${genePlatform.file.name}")

            def row = sql.firstRow("select title, organism from deapp.de_gpl_info where platform=${genePlatform.id}")
            if (!row) {
                if (genePlatform.title && genePlatform.organism) {
                    sql.execute("""\
							INSERT into deapp.de_gpl_info (PLATFORM, TITLE, ORGANISM, ANNOTATION_DATE, MARKER_TYPE)
							VALUES (?, ?, ?, current_timestamp, ?)
						""", [genePlatform.id, genePlatform.title, genePlatform.organism, genePlatform.platformType])
                } else {
                    throw new Exception("Cannot fetch platform title & organism for ${platform}")
                }
            } else {
                genePlatform.title = row.title
                genePlatform.organism = row.organism
            }

            config.logger.log("Platform: ${genePlatform.title} (${genePlatform.organism})")

            int lineNum = genePlatform.loadEntries(sql)

            if (lineNum == 0) throw new Exception("Platform file doesn't contain any EntrezGene IDs")

            if (!sql.connection.autoCommit) {
                sql.commit()
            }
            config.logger.log("Finished loading platform ${genePlatform.id}, processed ${lineNum} rows")

            studyInfo['runPlatformLoad'] = true
        }
    }
}
