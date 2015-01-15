package com.thomsonreuters.lsps.transmart.etl.platforms

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql

class MetabolomicsPlatform extends GenePlatform {
    MetabolomicsPlatform(File platformFile, String id, Object config) {
        super(new GplFile(platformFile), "METABOLOMICS", id, config)  // METABOLOMICS_ANNOT ?
    }

    @Override
    void cleanupTempTables(Sql sql) {
        sql.execute("DELETE FROM lt_metabolomic_annotation" as String)
    }

    @Override
    boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_metabolite_annotation WHERE gpl_id=?", [id])
        return row?.cnt
    }

    @Override
    int loadEntries(Sql sql) {
        return loadEachEntry(sql, """
            INSERT into lt_metabolomic_annotation (BIOCHEMICAL_NAME,HMDB_ID,SUPER_PATHWAY,SUB_PATHWAY, GPL_ID)
            VALUES (?, ?, ?, ?, ?)
        """) { entry ->
            [
                    entry.biochemical_name,
                    entry.hmdb_id,
                    entry.super_pathway,
                    entry.sub_pathway,
                    id
            ]
        }
    }

    @Override
    void eachEntry(Closure processEntry) {
        int biochemicalNameIdx = -1, hmdbIdIdx = -1, superPathwayIdx = -1, subPathwayIdx = -1
        def header = platformFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)BIOCHEMICAL/) biochemicalNameIdx = idx
            else if (val ==~ /(?i)HMDB[\s_]*ID/) hmdbIdIdx = idx
            else if (val ==~ /(?i)SUPER[\s_]PATHWAY/) superPathwayIdx = idx
            else if (val ==~ /(?i)SUB[\s_]PATHWAY/) subPathwayIdx = idx
        }
        if (biochemicalNameIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        config.logger.log(LogType.DEBUG, "BIOCHEMICAL_NAME, HMDB_ID, SUPER_PATHWAY, SUB_PATHWAY => " +
                "${header[biochemicalNameIdx]}, " +
                "${header[hmdbIdIdx]}, " +
                "${header[superPathwayIdx]}, " +
                "${header[subPathwayIdx]}")

        platformFile.eachEntry { String[] cols ->
            processEntry([
                    biochemical_name    : cols[biochemicalNameIdx],
                    hmdb_id             : hmdbIdIdx != -1 && !cols[hmdbIdIdx].isEmpty() ? cols[hmdbIdIdx] : null,
                    super_pathway       : superPathwayIdx != -1 ? cols[superPathwayIdx] : null,
                    sub_pathway         : subPathwayIdx != -1 ? cols[subPathwayIdx] : null
            ])

            return cols;
        }
    }
}
