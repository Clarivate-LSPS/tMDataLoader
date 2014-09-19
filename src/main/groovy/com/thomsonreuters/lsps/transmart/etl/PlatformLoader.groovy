package com.thomsonreuters.lsps.transmart.etl
import com.thomsonreuters.lsps.transmart.etl.platforms.GexPlatform
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

    void doLoad(File platformFile, String platform, studyInfo) {
        GexPlatform gexPlatform = new GexPlatform(platformFile, platform, config)
        gexPlatform.cleanupTempTables(sql)
        if (!gexPlatform.isLoaded(sql)) {
            // platform is not defined, loading
            config.logger.log("Loading platform: ${platform}")
            if (!platformFile.exists()) throw new Exception("Platform file not found: ${platformFile.name}")

            def row = sql.firstRow("select title, organism from deapp.de_gpl_info where platform=${platform}")
            if (!row) {
                if (gexPlatform.title && gexPlatform.organism) {
                    sql.execute("""\
							INSERT into deapp.de_gpl_info (PLATFORM, TITLE, ORGANISM, ANNOTATION_DATE, MARKER_TYPE)
							VALUES (?, ?, ?, current_timestamp, 'Gene Expression')
						""", [platform, gexPlatform.title, gexPlatform.organism])
                } else {
                    throw new Exception("Cannot fetch platform title & organism for ${platform}")
                }
            } else {
                gexPlatform.title = row.title
                gexPlatform.organism = row.organism
            }

            config.logger.log("Platform: ${gexPlatform.title} (${gexPlatform.organism})")

            int lineNum = gexPlatform.loadEntries(sql)

            if (lineNum == 0) throw new Exception("Platform file doesn't contain any EntrezGene IDs")

            if (!sql.connection.autoCommit) {
                sql.commit()
            }
            config.logger.log("Finished loading platform ${platform}, processed ${lineNum} rows")

            studyInfo['runPlatformLoad'] = true
        }
    }
}
