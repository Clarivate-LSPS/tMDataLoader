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
            else if (val ==~ /(?i)(GENE[\s_]*)*SYMBOL/) geneSymbolIdx = idx
            else if (val ==~ /(?i)SPECIES([\s_]*SCIENTIFIC)([\s_]*NAME)/) speciesIdx = idx
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

            if (cols[entrezGeneIdIdx].isEmpty() || cols[entrezGeneIdIdx] ==~ /\d+/) {
                logger.log(LogType.PROGRESS, "[${lineNum}]")
                processEntry([
                        probeset_id   : cols[0],
                        gene_symbol   : cols[geneSymbolIdx],
                        entrez_gene_id: !cols[entrezGeneIdIdx].isEmpty() ? cols[entrezGeneIdIdx] : null,
                        species       : speciesIdx != -1 ? cols[speciesIdx] : null
                ])
            }
            return cols;
        }
        logger.log(LogType.PROGRESS, "")
        return lineNum
    }

    static long eachMIRNAPlatformEntry(File platformFile, logger, Closure processEntry) {
        long lineNum = 0
        GplFile gplFile = new GplFile(platformFile)
        int mirnaIdIdx = -1, snIdIdx = -1, organismIdx = -1
        def header = gplFile.header
        header.eachWithIndex { String val, int idx ->
            if (val ==~ /(?i)(MIRNA[\s_]*)*ID/) mirnaIdIdx = idx
            else if (val ==~ /(?i)SN[\s_]*ID/) snIdIdx = idx
            else if (val ==~ /(?i)ORGANISM/) organismIdx = idx
        }
        if (organismIdx == -1) {
            // OK, trying to get species from the description
            logger.log(LogType.WARNING, "Organism not found in the platform file, using description")
        }
        if (mirnaIdIdx == -1 || snIdIdx == -1) {
            throw new Exception("Incorrect platform file header")
        }
        logger.log(LogType.DEBUG, "MIRNA_ID, SN_ID, ORGANISM => " +
                "${header[mirnaIdIdx]}, " +
                "${header[snIdIdx]}, " +
                "${organismIdx != -1 ? header[organismIdx] : '(Not specified)'}")

        gplFile.eachEntry { String[] cols ->
            lineNum++

            logger.log(LogType.PROGRESS, "[${lineNum}]")
            processEntry([
                    id_ref      : cols[0],
                    mirna_id    : !cols[mirnaIdIdx].isEmpty() ? cols[mirnaIdIdx] : null,
                    sn_id       : !cols[snIdIdx].isEmpty() ? cols[snIdIdx] : null,
                    organism    : organismIdx != -1 ? cols[organismIdx] : null
            ])

            return cols;
        }
        logger.log(LogType.PROGRESS, "")
        return lineNum
    }
}
