package com.thomsonreuters.lsps.transmart.etl.platforms

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql

class ProteinPlatform extends GenePlatform {
    ProteinPlatform(File platformFile, String id, Object config) {
        super(new GplFile(platformFile), "PROTEOMICS", id, config)
    }

    @Override
    void cleanupTempTables(Sql sql) {
        sql.execute("TRUNCATE TABLE ${config.loadSchema}.lt_protein_annotation" as String)
    }

    @Override
    boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_protein_annotation WHERE gpl_id=?", [id])
        return row?.cnt
    }

    @Override
    int loadEntries(Sql sql) {
        return loadEachEntry(sql, """
            INSERT into ${config.loadSchema}.lt_protein_annotation (PEPTIDE,UNIPROT_ID,ORGANISM,GPL_ID)
            VALUES (?, ?, ?, ?)
        """) { entry ->
            [
                    entry.peptide,
                    entry.uniprot_id,
                    entry.organism ?: organism,
                    id
            ]
        }
    }

    @Override
    void eachEntry(Closure processEntry) {
        int proteinIdIdx = -1, organismIdx = -1
        def header = platformFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)(MAJORITY[\s_]*)*(PROTEIN[\s_]*)*ID/) proteinIdIdx = idx
            else if (val ==~ /(?i)ORGANISM/) organismIdx = idx
        }
        if (organismIdx == -1) {
            // OK, trying to get species from the description
            config.logger.log(LogType.WARNING, "Organism not found in the platform file, using description")
        }
        if (proteinIdIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        config.logger.log(LogType.DEBUG, "MAJORITY_PROTEIN_ID, ORGANISM => " +
                "${header[proteinIdIdx]}, " +
                "${organismIdx != -1 ? header[organismIdx] : '(Not specified)'}")

        platformFile.eachEntry { String[] cols ->
            processEntry([
                    peptide     : cols[0],
                    uniprot_id  : !cols[proteinIdIdx].isEmpty() ? cols[proteinIdIdx] : null,
                    organism    : organismIdx != -1 ? cols[organismIdx] : null
            ])

            return cols;
        }
    }
}
