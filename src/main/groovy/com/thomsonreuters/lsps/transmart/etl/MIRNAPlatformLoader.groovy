package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql

class MIRNAPlatformLoader extends PlatformLoader {

    MIRNAPlatformLoader(Sql sql, Object config) {
        super(sql, config)
    }

    void doLoad(File platformFile, String platform, studyInfo, String mirnaType) {
        sql.execute("TRUNCATE TABLE ${config.loadSchema}.lt_qpcr_mirna_annotation" as String)

        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_qpcr_mirna_annotation WHERE gpl_id=?",
                [platform])
        if (!row?.cnt) {
            // platform is not defined, loading
            config.logger.log("Loading platform: ${platform}")
            if (!platformFile.exists()) throw new Exception("Platform file not found: ${platformFile.name}")

            def platformTitle = null
            def platformOrganism = null

            row = sql.firstRow("select title, organism from deapp.de_gpl_info where platform=${platform}")
            if (!row) {

                config.logger.log("Fetching platform description from GEO")
                def txt = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=${platform}".toURL().getText()

                def m = txt =~ /Title\<\/td\>\s*?\<td.*?\>(?:\[.+?\]\s*)*(.+?)\<\/td\>/
                if (m.size() > 0) {
                    platformTitle = m[0][1]
                }

                m = txt =~ /Organism\<\/td\>\s*?\<td.*?\>\<a.+?\>(.+?)\<\/a\>/
                if (m.size() > 0) {
                    platformOrganism = m[0][1]
                }

                GplFile gplFile = new GplFile(platformFile)
                if (!platformTitle) {
                    platformTitle = gplFile.metaInfo.PLATFORM_TITLE
                }
                if (!platformOrganism) {
                    platformOrganism = gplFile.metaInfo.PLATFORM_SPECIES ?: 'Homo Sapiens'
                }

                if (platformTitle && platformOrganism) {
                    sql.execute("""\
							INSERT into deapp.de_gpl_info (PLATFORM, TITLE, ORGANISM, ANNOTATION_DATE, MARKER_TYPE)
							VALUES (?, ?, ?, current_timestamp, ?)
						""", [platform, platformTitle, platformOrganism, mirnaType])
                } else {
                    throw new Exception("Cannot fetch platform title & organism for ${platform}")
                }
            } else {
                platformTitle = row.title
                platformOrganism = row.organism
            }

            config.logger.log("Platform: ${platformTitle} (${platformOrganism})")

            def lineNum = 0
            def isEmpty = true

            sql.withTransaction {
                sql.withBatch(500, """\
						INSERT into ${config.loadSchema}.lt_qpcr_mirna_annotation (ID_REF,MIRNA_ID,SN_ID,ORGANISM,GPL_ID)
						VALUES (?, ?, ?, ?, ?)
				""") {
                    stmt ->
                        lineNum = PlatformProcessor.eachMIRNAPlatformEntry(platformFile, config.logger) {
                            entry ->
                                // line with data
                                isEmpty = false
                                stmt.addBatch([
                                        entry.id_ref,
                                        entry.mirna_id,
                                        entry.sn_id,
                                        entry.organism ?: platformOrganism,
                                        platform
                                ])
                        }
                }
            }

            if (isEmpty) throw new Exception("Platform file doesn't contain any IDs")

            if (!sql.connection.autoCommit) {
                sql.commit()
            }
            config.logger.log("Finished loading platform ${platform}, processed ${lineNum} rows")

            studyInfo['runPlatformLoad'] = true
        }
    }
}
