package com.thomsonreuters.lsps.transmart

import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter

/**
 * Date: 27.04.2015
 * Time: 15:15
 */
class TdfUtils {
    static void transformColumnValue(int column, File oldFile, File newFile=null, Closure<String> transformValue) {
        transformCsvFile(oldFile, newFile) { CSVPrinter printer, String[] values ->
            for (int i = 0; i < values.length; i++) {
                if (i == column) {
                    printer.print(transformValue(values[i]))
                } else {
                    printer.print(values[i])
                }
            }
            printer.println()
        }
    }

    static void transformCsvFile(File oldFile, File newFile=null, Closure processRow) {
        if (newFile == null) {
            newFile = oldFile
        }
        boolean sameFile = newFile == oldFile
        if (sameFile) {
            oldFile = new File(oldFile.parentFile, "${oldFile.name}.bak")
            newFile.renameTo(oldFile)
        }
        CsvLikeFile csvMappingFile = new CsvLikeFile(oldFile)
        newFile.withWriter { expr ->
            def printer = new CSVPrinter(expr, CSVFormat.TDF.withHeader(csvMappingFile.header))
            csvMappingFile.eachEntry { String[] values ->
                processRow(printer, values)
            }
        }
        if (sameFile) {
            oldFile.delete()
        }
    }
}
