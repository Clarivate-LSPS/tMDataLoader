package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.files.GplFile
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
        sql.execute("TRUNCATE TABLE ${config.loadSchema}.lt_src_deapp_annot" as String)

        def row = sql.firstRow("SELECT count(*) as cnt FROM " + config.controlSchema + ".annotation_deapp WHERE gpl_id=?",
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
							VALUES (?, ?, ?, current_timestamp, 'Gene Expression')
						""", [platform, platformTitle, platformOrganism])
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
						INSERT into ${config.loadSchema}.lt_src_deapp_annot (GPL_ID,PROBE_ID,GENE_SYMBOL,GENE_ID,ORGANISM)
						VALUES (?, ?, ?, ?, ?)
				""") {
                    stmt ->
                        lineNum = PlatformProcessor.eachPlatformEntry(platformFile, config.logger) {
                            entry ->
                                // line with data
                                isEmpty = false
                                stmt.addBatch([
                                        platform,
                                        entry.probeset_id,
                                        entry.gene_symbol,
                                        entry.entrez_gene_id,
                                        entry.species ?: platformOrganism
                                ])
                        }
                }
            }

            if (isEmpty) throw new Exception("Platform file doesn't contain any EntrezGene IDs")

            if (!sql.connection.autoCommit) {
                sql.commit()
            }
            config.logger.log("Finished loading platform ${platform}, processed ${lineNum} rows")

            studyInfo['runPlatformLoad'] = true
        }
    }
}
