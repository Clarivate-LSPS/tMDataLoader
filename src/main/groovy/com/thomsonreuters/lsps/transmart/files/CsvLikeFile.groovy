package com.thomsonreuters.lsps.transmart.files
import com.thomsonreuters.lsps.utils.SkipLinesReader
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
/**
 * Created by bondarev on 3/28/14.
 */
class CsvLikeFile {
    File file
    protected String lineComment
    private List<String> header
    private List<String> headComments
    private boolean prepared

    CsvLikeFile(File file, String lineComment) {
        this.file = file
        this.lineComment = lineComment
    }

    String[] getHeader() {
        header ?: (header = file.withReader { reader ->
            String line;
            while ((line = reader.readLine()).startsWith(lineComment));
            line.split('\t', -1)
        })
    }

    String[] getHeadComments() {
        headComments ?: (headComments = file.withReader { reader ->
            String line
            List<String> headComments = []
            while ((line = reader.readLine()).startsWith(lineComment)) {
                headComments << line.substring(lineComment.length()).trim()
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

    protected final void prepareIfRequired() {
        if (!prepared) {
            prepare()
            prepared = true
        }
    }

    protected void prepare() {
    }

    def <T> T eachEntry(Closure<T> processEntry) {
        prepareIfRequired()
        file.withReader { reader ->
            def skipLinesReader = new SkipLinesReader(reader, [lineComment])
            def format = CSVFormat.TDF.withHeader().withSkipHeaderRecord(true)
            def parser = new CSVParser(skipLinesReader, format)
            for (CSVRecord record : parser) {
                processEntry(makeEntry(record))
            }
        }
    }
}
