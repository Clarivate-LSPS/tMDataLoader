package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.files.GplFile

/**
 * Created by bondarev on 3/28/14.
 */
class PlatformProcessor {
    static long eachPlatformEntry(File platformFile, logger, Closure processEntry) {
        long lineNum = 0
        GplFile gplFile = new GplFile(platformFile)
        int entrezGeneIdIdx = -1, geneSymbolIdx = -1, speciesIdx = -1
        def header = gplFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)(ENTREZ[\s_]*)*GENE([\s_]*ID)*/) entrezGeneIdIdx = idx
            if (val ==~ /(?i)(GENE[\s_]*)*SYMBOL/) geneSymbolIdx = idx
            if (val ==~ /(?i)SPECIES([\s_]*SCIENTIFIC)([\s_]*NAME)/) speciesIdx = idx
        }
        if (speciesIdx == -1) {
            // OK, trying to get species from the description
            logger.log(LogType.WARNING, "Species not found in the platform file, using description")
        }
        if (entrezGeneIdIdx == -1 || geneSymbolIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        logger.log(LogType.DEBUG, "ENTREZ, SYMBOL, SPECIES => " +
                "${header[entrezGeneIdIdx]}, " +
                "${header[geneSymbolIdx]}, " +
                "${speciesIdx != -1 ? header[speciesIdx] : '(Not specified)'}")

        gplFile.eachEntry { String[] cols ->
            lineNum++

            if (cols[entrezGeneIdIdx] ==~ /\d+/) {
                logger.log(LogType.PROGRESS, "[${lineNum}]")
                processEntry([
                        probeset_id   : cols[0],
                        gene_symbol   : cols[geneSymbolIdx],
                        entrez_gene_id: cols[entrezGeneIdIdx],
                        species       : speciesIdx != -1 ? cols[speciesIdx] : null
                ])
            }
            return cols;
        }
        logger.log(LogType.PROGRESS, "")
        return lineNum
    }
}
