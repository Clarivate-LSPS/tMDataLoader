package com.thomsonreuters.lsps.transmart.etl.platforms
import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql
/**
 * Date: 19.09.2014
 * Time: 12:47
 */
class GexPlatform extends GenePlatform {
    GexPlatform(File platformFile, String id, Object config) {
        super(new GplFile(platformFile), id, config)
    }

    @Override
    public void cleanupTempTables(Sql sql) {
        sql.execute("TRUNCATE TABLE ${config.loadSchema}.lt_src_deapp_annot" as String)
    }

    @Override
    public boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_mrna_annotation WHERE gpl_id=?", [id])
        return row?.cnt
    }


    @Override
    public int loadEntries(Sql sql) {
        return loadEachEntry(sql, """
            INSERT into ${config.loadSchema}.lt_src_deapp_annot (GPL_ID,PROBE_ID,GENE_SYMBOL,GENE_ID,ORGANISM)
            VALUES (?, ?, ?, ?, ?)
        """) { entry ->
            [
                    id,
                    entry.probeset_id,
                    entry.gene_symbol,
                    entry.entrez_gene_id,
                    entry.species ?: organism
            ]
        }
    }

    @Override
    public void eachEntry(Closure processEntry) {
        int entrezGeneIdIdx = -1, geneSymbolIdx = -1, speciesIdx = -1
        def header = platformFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)(ENTREZ[\s_]*)*GENE([\s_]*ID)*/) entrezGeneIdIdx = idx
            else if (val ==~ /(?i)(GENE[\s_]*)*SYMBOL/) geneSymbolIdx = idx
            else if (val ==~ /(?i)SPECIES([\s_]*SCIENTIFIC)([\s_]*NAME)/) speciesIdx = idx
        }
        if (speciesIdx == -1) {
            // OK, trying to get species from the description
            config.logger.log(LogType.WARNING, "Species not found in the platform file, using description")
        }
        if (entrezGeneIdIdx == -1 || geneSymbolIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        config.logger.log(LogType.DEBUG, "ENTREZ, SYMBOL, SPECIES => " +
                "${header[entrezGeneIdIdx]}, " +
                "${header[geneSymbolIdx]}, " +
                "${speciesIdx != -1 ? header[speciesIdx] : '(Not specified)'}")

        platformFile.eachEntry { String[] cols ->
            if (cols[entrezGeneIdIdx].isEmpty() || cols[entrezGeneIdIdx] ==~ /\d+/) {
                processEntry([
                        probeset_id   : cols[0],
                        gene_symbol   : cols[geneSymbolIdx],
                        entrez_gene_id: !cols[entrezGeneIdIdx].isEmpty() ? cols[entrezGeneIdIdx] : null,
                        species       : speciesIdx != -1 ? cols[speciesIdx] : null
                ])
            }
            return cols;
        }
    }
}
