package com.thomsonreuters.lsps.transmart.etl.platforms

import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.files.GplFile
import groovy.sql.Sql

import java.nio.file.Path

/**
 * Created by transmart on 3/5/15.
 */
class aCGHPlatform extends GenePlatform{

    aCGHPlatform(Path platformFile, String id, Object config) {
        super(new GplFile(platformFile), 'aCGH', id, config)
    }

    @Override
    void cleanupTempTables(Sql sql) {
        sql.execute("DELETE FROM lt_chromosomal_region" as String)
    }

    @Override
    boolean isLoaded(Sql sql) {
        def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_chromosomal_region WHERE gpl_id = ?",[id])
        return row?.cnt
    }

    @Override
    int loadEntries(Sql sql) {
        return loadEachEntry(sql, """
            INSERT into lt_chromosomal_region (gpl_id, chromosome, start_bp, end_bp, num_probes, region_name, cytoband, gene_symbol, gene_id, organism)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             """) { entry ->
            [
                    id,
                    entry.chromosome,
                    new Integer((String)entry.start_bp),
                    new Integer((String)entry.end_bp),
                    new Integer((String)entry.num_probes),
                    entry.region_name,
                    entry.cytoband,
                    entry.gene_symbol,
                    entry.gene_id?new Integer((String)entry.gene_id):null,
                    entry.species ?: organism
            ]
        }
    }

    @Override
    void eachEntry(Closure processEntry) {
        platformFile.eachEntry { String[] cols ->
            processEntry([
                    chromosome 		: cols[2],
                    start_bp 		: cols[3],
                    end_bp 			: cols[4],
                    num_probes 		: cols[5],
                    region_name		: cols[1],
                    cytoband		: cols[6],
                    gene_symbol		: cols[7],
                    gene_id			: (cols[8].isEmpty()?null:cols[8]),
                    organism		: cols[9]
            ])

            return cols;
        }
    }
}
