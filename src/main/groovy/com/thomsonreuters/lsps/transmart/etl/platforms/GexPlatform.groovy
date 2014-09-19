package com.thomsonreuters.lsps.transmart.etl.platforms
import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.files.GplFile
import com.thomsonreuters.lsps.transmart.files.MetaInfoHeader
import com.thomsonreuters.lsps.transmart.util.PrepareIfRequired
import com.thomsonreuters.lsps.transmart.util.annotations.RequiresPrepare
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
/**
 * Date: 19.09.2014
 * Time: 12:47
 */
@Mixin(PrepareIfRequired)
class GexPlatform {
    String id
    @RequiresPrepare
    String title
    @RequiresPrepare
    String organism
    def config
    private CsvLikeFile platformFile

    GexPlatform(File platformFile, String id, config) {
        this.platformFile = new GplFile(platformFile)
        this.id = id
        this.config = config
    }

    protected Map fetchPlatformInfo() {
        config.logger.log("Fetching platform description from GEO")
        def txt = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=${id}".toURL().getText()

        String title, organism
        def m = txt =~ /Title\<\/td\>\s*?\<td.*?\>(?:\[.+?\]\s*)*(.+?)\<\/td\>/
        if (m.size() > 0) {
            title = m[0][1]
        }

        m = txt =~ /Organism\<\/td\>\s*?\<td.*?\>\<a.+?\>(.+?)\<\/a\>/
        if (m.size() > 0) {
            organism = m[0][1]
        }
        return [title: title, organism: organism]
    }

    protected void readPlatformInfo() {
        use(MetaInfoHeader) {
            title = title ?: platformFile.metaInfo.PLATFORM_TITLE
            organism = organism ?: platformFile.metaInfo.PLATFORM_SPECIES
        }
        if (!title) {
            def info = fetchPlatformInfo()
            title = info.title
            if (info.organism) {
                organism = info.organism
            }
        }
        organism = organism ?: 'Homo Sapiens'
    }

    protected void prepare() {
        readPlatformInfo()
    }

    public void cleanupTempTables(Sql sql) {
        sql.execute("TRUNCATE TABLE ${config.loadSchema}.lt_src_deapp_annot" as String)
    }

    public boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_mrna_annotation WHERE gpl_id=?", [id])
        return row?.cnt
    }

    protected int loadEachEntry(Sql sql, String insertStatement, Closure<List> toList) {
        int lineNum = 0
        sql.withTransaction {
            sql.withBatch(500, insertStatement) { BatchingPreparedStatementWrapper stmt ->
                eachEntry {
                    lineNum++
                    stmt.addBatch(toList(it))
                    config.logger.log(LogType.PROGRESS, "[${lineNum}]")
                }
            }
        }
        config.logger.log(LogType.PROGRESS, "")
        return lineNum
    }

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
