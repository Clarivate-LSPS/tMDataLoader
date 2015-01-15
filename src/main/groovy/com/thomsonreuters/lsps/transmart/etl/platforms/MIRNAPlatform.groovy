package com.thomsonreuters.lsps.transmart.etl.platforms

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql

/**
 * Date: 19.09.2014
 * Time: 17:52
 */
class MIRNAPlatform extends GenePlatform {
    MIRNAPlatform(File platformFile, String id, String mirnaType, Object config) {
        super(new GplFile(platformFile), mirnaType, id, config)
    }

    @Override
    void cleanupTempTables(Sql sql) {
        sql.execute("DELETE FROM lt_qpcr_mirna_annotation" as String)
    }

    @Override
    boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_qpcr_mirna_annotation WHERE gpl_id=?", [id])
        return row?.cnt
    }

    @Override
    int loadEntries(Sql sql) {
        return loadEachEntry(sql, """
            INSERT into lt_qpcr_mirna_annotation (ID_REF,MIRNA_ID,SN_ID,ORGANISM,GPL_ID)
            VALUES (?, ?, ?, ?, ?)
        """) { entry ->
            [
                    entry.id_ref,
                    entry.mirna_id,
                    entry.sn_id,
                    entry.organism ?: organism,
                    id
            ]
        }
    }

    @Override
    void eachEntry(Closure processEntry) {
        int mirnaIdIdx = -1, snIdIdx = -1, organismIdx = -1
        def header = platformFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)(MIRNA[\s_]*)*ID/) mirnaIdIdx = idx
            else if (val ==~ /(?i)SN[\s_]*ID/) snIdIdx = idx
            else if (val ==~ /(?i)ORGANISM/) organismIdx = idx
        }
        if (organismIdx == -1) {
            // OK, trying to get species from the description
            config.logger.log(LogType.WARNING, "Organism not found in the platform file, using description")
        }
        if (mirnaIdIdx == -1 || snIdIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        config.logger.log(LogType.DEBUG, "MIRNA_ID, SN_ID, ORGANISM => " +
                "${header[mirnaIdIdx]}, " +
                "${header[snIdIdx]}, " +
                "${organismIdx != -1 ? header[organismIdx] : '(Not specified)'}")

        platformFile.eachEntry { String[] cols ->
            processEntry([
                    id_ref      : cols[0],
                    mirna_id    : !cols[mirnaIdIdx].isEmpty() ? cols[mirnaIdIdx] : null,
                    sn_id       : !cols[snIdIdx].isEmpty() ? cols[snIdIdx] : null,
                    organism    : organismIdx != -1 ? cols[organismIdx] : null
            ])

            return cols;
        }
    }
}
