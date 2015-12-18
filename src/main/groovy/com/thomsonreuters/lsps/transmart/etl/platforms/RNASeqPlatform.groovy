package com.thomsonreuters.lsps.transmart.etl.platforms

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql

import java.nio.file.Path

class RNASeqPlatform extends GenePlatform {
    RNASeqPlatform(Path platformFile, String id, Object config) {
        super(new GplFile(platformFile), "RNASEQ", id, config)
    }

    @Override
    void cleanupTempTables(Sql sql) {
        sql.execute("DELETE FROM lt_rnaseq_annotation" as String)
    }

    @Override
    boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_rnaseq_annotation WHERE gpl_id=?", [id])
        return row?.cnt
    }

    @Override
    int loadEntries(Sql sql) {
        return loadEachEntry(sql, """
            INSERT into lt_rnaseq_annotation (TRANSCRIPT_ID,GENE_SYMBOL,ORGANISM)
            VALUES (?, ?, ?)
        """) { entry ->
            [
                    entry.transcript_id,
                    entry.gene_symbol,
                    entry.organism,
            ]
        }
    }

    @Override
    void eachEntry(Closure processEntry) {
        int transcriptIdIdx = -1, geneSymbolIdx = -1, organismIdx = -1
        def header = platformFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)(TRANSCRIPT[\s_]*)*ID/) transcriptIdIdx = idx
            else if (val ==~ /(?i)(GENE[\s_]*)*SYMBOL/) geneSymbolIdx = idx
            else if (val ==~ /(?i)ORGANISM/) organismIdx = idx
        }
        if (transcriptIdIdx == -1 || geneSymbolIdx == -1 || organismIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        config.logger.log(LogType.DEBUG, "TRANSCRIPT_ID, GENE_SYMBOL, ORGANISM => " +
                "${header[transcriptIdIdx]}, " +
                "${header[geneSymbolIdx]}, "  +
                "${header[organismIdx]} ")

        platformFile.eachEntry { String[] cols ->
            processEntry([
                    transcript_id: cols[transcriptIdIdx],
                    gene_symbol  : cols[geneSymbolIdx],
                    organism     : cols[organismIdx]
            ])

            return cols;
        }
    }
}
