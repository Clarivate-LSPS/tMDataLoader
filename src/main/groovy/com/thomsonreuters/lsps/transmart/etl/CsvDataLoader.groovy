package com.thomsonreuters.lsps.transmart.etl

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter

/**
 * Created by bondarev on 4/21/14.
 */
class CsvDataLoader extends DataLoader {
    private static class BatchWriter {
        private CSVPrinter printer

        BatchWriter(PrintWriter out) {
            printer = new CSVPrinter(out, CSVFormat.TDF.withRecordSeparator(System.getProperty("line.separator")))
        }

        def addBatch(Object[] data) {
            addBatch(data as List)
        }

        def addBatch(List data) {
            for (def value : data) {
                if (value instanceof Boolean) {
                    value = value ? 't' : 'f'
                }
                printer.print(value)
            }
            printer.println()
        }
    }

    @Override
    def withBatch(Closure block) {
        String command = "COPY ${tableName}"
        if (columnNames) {
            command += "(${columnNames.join(', ')})"
        }
        command += " FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')"
        database.withSql { sql->
            OutputStream out = org.postgresql.copy.PGCopyOutputStream.newInstance([sql.connection, command as String] as Object[])
            def printer = new PrintWriter(out)
            def result = block.call(new BatchWriter(printer))
            printer.println "\\."
            printer.flush()
            printer.close()
            return result
        }
    }
}
