package com.thomsonreuters.lsps.transmart.files
import com.thomsonreuters.lsps.transmart.util.PrepareIfRequired
import com.thomsonreuters.lsps.utils.SkipLinesReader
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
/**
 * Created by bondarev on 3/28/14.
 */
class CsvLikeFile implements PrepareIfRequired {
    File file
    protected String lineComment
    private List<String> header
    private List<String> headComments
    protected CSVFormat format = CSVFormat.TDF.
            withHeader().
            withSkipHeaderRecord(true).
            withIgnoreEmptyLines(true).
            withAllowMissingColumnNames(true)

    protected def withParser(Closure closure) {
        file.withReader { reader ->
            def skipLinesReader = !lineComment.is(null) ? new SkipLinesReader(reader, [lineComment]) : reader
            def parser = new CSVParser(skipLinesReader, format)
            closure.call(parser)
        }
    }

    CsvLikeFile(File file, String lineComment = null) {
        this.file = file
        this.lineComment = lineComment
    }

    String[] getHeader() {
        header ?: (header = withParser { it.headerMap.keySet() as String[] })
    }

    String[] getHeadComments() {
        headComments ?: (headComments = file.withReader { reader ->
            List<String> headComments = []
            if (lineComment != null) {
                String line
                while ((line = reader.readLine()).startsWith(lineComment)) {
                    headComments << line.substring(lineComment.length()).trim()
                }
            }
            headComments
        })
    }

    protected def makeEntry(CSVRecord record) {
        String[] entry = new String[record.size()]
        for (int i = 0; i < record.size(); i++) {
            entry[i] = record.get(i)
        }
        return entry
    }

    def <T> T eachEntry(Closure<T> processEntry) {
        prepareIfRequired()
        withParser { CSVParser parser ->
            for (CSVRecord record : parser) {
                processEntry(makeEntry(record))
            }
        }
    }
}
