package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

/**
 * Created by bondarev on 4/8/14.
 */
class CsvFileLoader {
    Sql sql
    CharSequence table
    Collection<CharSequence> columns
    Logger logger

    CsvFileLoader(Sql sql, CharSequence table, Collection<CharSequence> columns) {
        this.sql = sql
        this.table = table
        this.columns = columns
    }

    void loadFile(File f, Closure prepareEntry = Closure.IDENTITY) {
        String insertCommand = "insert into ${table}(${columns.join(',')}) values (${columns.collect { '?' }.join(',')})"
        long lineNum = 0
        sql.withBatch(500, insertCommand) { stmt ->
            f.splitEachLine('\t') { entry ->
                lineNum++
                if (lineNum <= 1) {
                    return
                }
                logger?.log(LogType.PROGRESS, "[${lineNum}]")
                stmt.addBatch(prepareEntry(entry))
            }
        }
        logger?.log(LogType.PROGRESS, '')
        sql.commit()
    }
}
