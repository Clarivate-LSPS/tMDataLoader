package com.thomsonreuters.lsps.transmart.files

import com.thomsonreuters.lsps.transmart.etl.DataProcessingException
import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.etl.Logger
import com.thomsonreuters.lsps.transmart.util.PrepareIfRequired
import com.thomsonreuters.lsps.utils.SkipLinesReader
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord

import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CodingErrorAction
import java.nio.file.Path

/**
 * Created by bondarev on 3/28/14.
 */
class CsvLikeFile implements PrepareIfRequired {
    private static final Logger logger = Logger.getLogger(CsvLikeFile.class)

    Path file
    protected String lineComment
    private List<String> header
    private boolean allowNonUniqueColumnNames
    private List<String> headComments
    protected CSVFormat format = CSVFormat.TDF.
            withSkipHeaderRecord(true).
            withIgnoreEmptyLines(true).
            withIgnoreSurroundingSpaces(true).
            withAllowMissingColumnNames(true)

    protected def withParser(CSVFormat format = null, Closure closure) {
            file.withInputStream { inputStream ->
                CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder()
                        .onMalformedInput(CodingErrorAction.REPLACE)
                        .onUnmappableCharacter(CodingErrorAction.REPLACE);
                InputStreamReader reader = new InputStreamReader(inputStream, decoder);

                def linesReader = !lineComment.is(null) ? new SkipLinesReader(reader, [lineComment]) : reader
                def parser
                try {
                    parser = new CSVParser(linesReader, format ?: this.format)
                } catch (IllegalArgumentException ex) {
                    if (ex.message.contains("The header contains a duplicate name")) {
                        throw new IllegalArgumentException("Duplicate names found in the header. You should either " +
                                "check and fix these names or use `--allow-non-unique-columns` option", ex)
                    } else {
                        throw ex
                    }
                }
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

    CsvLikeFile(Path file, String lineComment = null, boolean allowNonUniqueColumnNames = false) {
        this.file = file
        this.lineComment = lineComment
        this.allowNonUniqueColumnNames = allowNonUniqueColumnNames
        try {
            this.header = getHeader()
        }
        catch (IOException e) {
            logger.log(LogType.ERROR, "Error parsing header of ${file} file.")
            throw new DataProcessingException("Error parsing header of ${file} file.")
        }
    }

    CSVFormat getFormat() {
        return this.format
    }

    String[] getHeader() {
        header ?: (header = withParser(format.withHeader((String[]) null)) {
            it.nextRecord().toList() }
        )
    }

    String[] getHeadComments() {
        headComments ?: (headComments = file.withReader { reader ->
            HeadCommentsReader.readHeadComments(reader, lineComment)
        })
    }

    protected def makeEntry(String[] values) {
        return values
    }

    protected String[] getRecordValues(CSVRecord record) {
        String[] values = new String[record.size()]
        for (int i = 0; i < record.size(); i++) {
            values[i] = record.get(i)
        }
        return values
    }

    private List<String> getRefinedHeader() {
        List<String> refinedHeader = new ArrayList<String>(header)
        int idx = refinedHeader.size()
        refinedHeader.reverseEach {
            idx--
            for (int pos = 0; pos < idx; pos++) {
                if (refinedHeader[pos].equals(it)) {
                    refinedHeader[pos] = it + '@' + pos
                    refinedHeader[idx] = it + '@' + idx
                    break
                }
            }
        }
        return refinedHeader
    }

    def <T> T eachEntry(Closure<T> processEntry) {
        prepareIfRequired()
        CSVFormat format = format
        if (!allowNonUniqueColumnNames) {
            format = format.withHeader()
        } else {
            format = format.withHeader(refinedHeader as String[])
        }
        withParser(format) { CSVParser parser, lineNumberProducer ->
            def _processEntry = processEntry.maximumNumberOfParameters == 2 ?
                    { processEntry(it, lineNumberProducer()) } :
                    { processEntry(it) }
            try {
                for (CSVRecord record : parser) {
                    String[] values = getRecordValues(record)
                    if (!record.consistent) {
                        String prefix = "Line [${lineNumberProducer()}] is inconsistent - "
                        if (values.every { it.isEmpty() }) {
                            logger.log(LogType.WARNING, prefix + "ignored (all values is empty).")
                            continue
                        } else if (values.length > parser.headerMap.size()) {
                            String[] extraValues = Arrays.copyOfRange(values, parser.headerMap.size(), record.size())
                            if (extraValues.every { it.isEmpty() }) {
                                logger.log(LogType.WARNING, prefix + "it has extra empty values.")
                            } else {
                                throw new RuntimeException(prefix + "it has extra values: ${extraValues} (values: ${record.toMap()})" as String)
                            }
                        } else {
                            def missingColumns = parser.headerMap.keySet() - record.toMap().keySet()
                            if (missingColumns.every { it.isEmpty() }) {
                                logger.log(LogType.WARNING, prefix + "it has missing values for untitled columns")
                            } else {
                                logger.log(LogType.WARNING, prefix + "it has missing values for columns ${missingColumns}, assume they are empty")
                                values = Arrays.copyOf(values, parser.headerMap.size())
                                Arrays.fill(values, record.size(), values.length, '')
                            }
                        }
                    }
                    _processEntry(makeEntry(values))
                }
            }
            catch (IOException e){
                throw new DataProcessingException("Error while processing line [${lineNumberProducer()}].").initCause(e)
            }
        }
    }
}
