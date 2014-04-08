package com.thomsonreuters.lsps.transmart.files

import sun.misc.Regexp

import java.util.regex.Pattern

/**
 * Created by bondarev on 3/28/14.
 */
class CsvLikeFile {
    File file
    protected String lineComment
    private List<String> header
    private List<String> headComments
    private final Pattern separator = Pattern.compile('\t')
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

    protected def makeEntry(String line) {
        separator.split(line, -1)
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
        boolean headerSkipped = false
        file.eachLine { line ->
            if (line.isEmpty() || line.startsWith(lineComment)) {
                return null;
            }
            if (!headerSkipped) {
                headerSkipped = true;
                return null;
            }
            processEntry(makeEntry(line))
        }
    }
}
