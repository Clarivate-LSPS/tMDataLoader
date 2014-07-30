package com.thomsonreuters.lsps.transmart.etl

/**
 * Created by bondarev on 4/21/14.
 */
class CsvDataLoader extends DataLoader {
    private static class BatchWriter {
        private PrintWriter out
        private StringBuilder buf = new StringBuilder()

        BatchWriter(PrintWriter out) {
            this.out = out
        }

        def addBatch(Object[] data) {
            addBatch(data as List)
        }

        def addBatch(List data) {
            buf.setLength(0)
            for (int i = 0; i < data.size(); i++) {
                def value = data[i]
                switch (value) {
                    case Integer:
                    case Long:
                    case Float:
                    case Double:
                        buf.append(value)
                        break
                    case String:
                    case GString:
                        value = (value as String).replaceAll(/"/, '""')
                        if (value.indexOf('\t') != -1) {
                            buf.append('\"').append(value).append('\"')
                        } else {
                            buf.append(value)
                        }
                        break
                    case Boolean:
                        buf.append(value ? 't' : 'f')
                        break
                    case null:
                        break
                    default:
                        throw new RuntimeException("Invalid value type: ${data[i].class}")
                }
                buf.append('\t')
            }
            buf.setLength(buf.length() - 1)
            out.println(buf)
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
