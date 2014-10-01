package com.thomsonreuters.lsps.transmart.files

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.etl.Logger
import com.thomsonreuters.lsps.transmart.util.PrepareIfRequired
import com.thomsonreuters.lsps.utils.SkipLinesReader
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord

/**
 * Created by bondarev on 3/28/14.
 */
class CsvLikeFile implements PrepareIfRequired {
    private static final Logger logger = new Logger([:])

    File file
    protected String lineComment
    private List<String> header
    private List<String> headComments
    protected CSVFormat format = CSVFormat.TDF.
            withHeader().
            withSkipHeaderRecord(true).
            withIgnoreEmptyLines(true).
            withIgnoreSurroundingSpaces(true).
            withAllowMissingColumnNames(true)

    protected def withParser(Closure closure) {
        file.withReader { reader ->
            def linesReader = !lineComment.is(null) ? new SkipLinesReader(reader, [lineComment]) : reader
            def parser = new CSVParser(linesReader, format)
            if (closure.maximumNumberOfParameters == 2) {
                def lineNumberProducer = linesReader instanceof SkipLinesReader ?
                        { (linesReader as SkipLinesReader).skippedLinesCount + parser.currentLineNumber } :
                        parser.&getCurrentLineNumber
                closure.call(parser, lineNumberProducer)
            } else {
                closure.call(parser)
            }
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
        withParser { CSVParser parser, lineNumberProducer ->
            def _processEntry = processEntry.maximumNumberOfParameters == 2 ?
                    { processEntry(makeEntry(it), lineNumberProducer()) } :
                    { processEntry(makeEntry(it)) }
            for (CSVRecord record : parser) {
                if (!record.consistent) {
                    List<String> values = record.iterator().toList()
                    if (values.every { it.isEmpty() }) {
                        logger.log(LogType.WARNING, "Line [${lineNumberProducer()}] ignored. It is inconsistent, but all values is empty.")
                        continue
                    } else if (record.size() > parser.headerMap.size()) {
                        def extraValues = values.subList(parser.headerMap.size(), values.size())
                        if (extraValues.every { it.isEmpty() }) {
                            logger.log(LogType.WARNING, "Line [${lineNumberProducer()}] is inconsistent - it has extra empty values.")
                        } else {
                            throw new RuntimeException("Line [${lineNumberProducer()}] is inconsistent - it has extra values: ${extraValues}")
                        }
                    } else {
                        def errorMessage = "Line [${lineNumberProducer()}] is inconsistent - ${record.toMap()}, missing columns: ${parser.headerMap.keySet() - record.toMap().keySet()}"
                        throw new RuntimeException(errorMessage)
                    }
                }
                _processEntry(record)
            }
        }
    }
}
