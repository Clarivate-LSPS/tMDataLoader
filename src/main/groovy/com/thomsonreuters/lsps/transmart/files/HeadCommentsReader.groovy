package com.thomsonreuters.lsps.transmart.files

/**
 * Date: 21-Apr-16
 * Time: 17:13
 */
class HeadCommentsReader {
    static String[] readHeadComments(Reader reader, String lineComment = null) {
        List<String> headComments = []
        if (lineComment != null) {
            String line
            while ((line = reader.readLine())?.startsWith(lineComment)) {
                headComments << line.substring(lineComment.length()).trim()
            }
        }
        headComments
    }
}
