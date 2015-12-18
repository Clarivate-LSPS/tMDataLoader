package com.thomsonreuters.lsps.transmart.etl.platforms

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql

import java.nio.file.Path

class RBMPlatform extends GenePlatform {
    RBMPlatform(Path platformFile, String id, Object config) {
        super(new GplFile(platformFile), "RBM", id, config)
    }

    @Override
    void cleanupTempTables(Sql sql) {
        sql.execute("DELETE FROM lt_src_rbm_annotation" as String)
    }

    @Override
    boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_rbm_annotation WHERE gpl_id=?", [id])
        return row?.cnt
    }

    @Override
    int loadEntries(Sql sql) {
        return loadEachEntry(sql, """
            INSERT into lt_src_rbm_annotation(ANTIGEN_NAME,UNIPROTID,GENE_SYMBOL,GENE_ID,GPL_ID)
            VALUES (?, ?, ?, ?, ?)
        """) { entry ->
            [
                    entry.antigen_name,
                    entry.uniprot_id,
                    entry.gene_symbol,
                    entry.gene_id,
                    id
            ]
        }
    }

    @Override
    void eachEntry(Closure processEntry) {
        int antigenNameIdx = -1, uniprotIdIdx = -1, geneSymbolIdx = -1, geneIdIdx = -1
        def header = platformFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)ANTIGEN[\s_]*NAME/) antigenNameIdx = idx
            else if (val ==~ /(?i)UNIPROT/) uniprotIdIdx = idx
            else if (val ==~ /(?i)(GENE[\s_]*)*SYMBOL/) geneSymbolIdx = idx
            else if (val ==~ /(?i)GENE[\s_]*ID/) geneIdIdx = idx
        }
        if (antigenNameIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        config.logger.log(LogType.DEBUG, "ANTIGEN_NAME, UNIPROTID, GENE_SYMBOL, GENE_ID => " +
                "${header[antigenNameIdx]}, " +
                "${uniprotIdIdx != -1 ? header[uniprotIdIdx] : '(Not specified)'}, " +
                "${geneSymbolIdx != -1 ? header[geneSymbolIdx] : '(Not specified)'}, " +
                "${geneIdIdx != -1 ? header[geneIdIdx] : '(Not specified)'}, ")

        platformFile.eachEntry { String[] cols ->
            processEntry([
                    antigen_name: cols[antigenNameIdx],
                    uniprot_id  : uniprotIdIdx != -1 ? cols[uniprotIdIdx] : null,
                    gene_symbol : geneSymbolIdx != -1 ? cols[geneSymbolIdx] : null,
                    gene_id     : geneIdIdx != -1 ? cols[geneIdIdx] : null
            ])

            return cols;
        }
    }
}
