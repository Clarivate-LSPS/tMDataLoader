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
                        if ((value as String).indexOf('\t') != -1) {
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
        Process runner = Runtime.runtime.exec(
                ["psql", "-h", database.host, "-U", database.config.username,
                 "-d", database.database, "-c", command] as String[],
                ["PGPASSWORD=${database.config.password}"] as String[])
        runner.consumeProcessOutput(System.out, System.err)
        def printer = new PrintWriter(runner.out)
        def result = block.call(new BatchWriter(printer))
        printer.println "\\."
        printer.flush()
        printer.close()
        runner.waitFor()
        if (runner.exitValue() != 0) {
            throw new RuntimeException(runner.errorStream.text)
        }
        return result
    }
}
