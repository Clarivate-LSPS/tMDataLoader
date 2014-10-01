package com.thomsonreuters.lsps.utils

/**
 * Date: 11.09.2014
 * Time: 18:48
 */
class SkipLinesReader extends Reader {
    private BufferedReader reader
    private boolean atLineStart = true;
    private char[] prefixBuffer;

    private static final char CR = '\r';
    private static final char NL = '\n';
    private static final char[] EMPTY_BUFFER = new char[0];
    private SortedSet<CharSequence> prefixes = new TreeSet<>();
    private int skippedLinesCount;

    SkipLinesReader(Reader reader, Collection<String> skipPrefixes) {
        this.reader = reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader)
        this.prefixes.addAll(skipPrefixes);
        this.prefixBuffer = new char[this.prefixes.last().length()];
    }

    int getSkippedLinesCount() {
        return skippedLinesCount
    }

    private void skipLines() {
        skipLines(EMPTY_BUFFER, 0, 0)
    }

    @Override
    int read() throws IOException {
        if (atLineStart) {
            skipLines();
        }
        int c = reader.read();
        atLineStart = c == NL;
        return c;
    }

    private int skipToNextLine(char[] buf, int off, int end) {
        skippedLinesCount++;
        while (off < end) {
            if (buf[off++] == NL) {
                return off;
            }
        }
        // If no line end in buffer, advance reader to line end
        reader.readLine();
        return off;
    }

    private int readPrefixBuffer(char[] buf, int off, int end) {
        int prefixSize = Math.min(end - off, prefixBuffer.length);
        System.arraycopy(buf, off, prefixBuffer, 0, Math.min(prefixBuffer.length, prefixSize))
        if (prefixSize < prefixBuffer.length) {
            reader.mark(prefixBuffer.length - prefixSize);
            int read = reader.read(prefixBuffer, prefixSize, prefixBuffer.length - prefixSize);
            if (read > 0) {
                prefixSize += read;
            }
            reader.reset();
        }
        return prefixSize;
    }

    private int skipLines(char[] buf, int off, int end) {
        lines:
        while (true) {
            int avail = readPrefixBuffer(buf, off, end);
            prefixes:
            for (CharSequence prefix : prefixes) {
                if (prefix.length() > avail) {
                    break;
                }
                for (int i = 0; i < prefix.length(); i++) {
                    if (prefix.charAt(i) != prefixBuffer[i]) {
                        continue prefixes;
                    }
                }
                off = skipToNextLine(buf, off + avail, end)
                continue lines;
            }
            break;
        }
        return off;
    }

    @Override
    int read(char[] cbuf, int off, int len) throws IOException {
        int count = reader.read(cbuf, off, len);
        if (count < 0) {
            return count;
        }
        int start = off
        int end = off + count;
        while (off < end) {
            if (atLineStart) {
                int skipTo = skipLines(cbuf, off, end);
                int hasToRead = skipTo - off
                if (hasToRead > 0) {
                    if (skipTo < end) {
                        System.arraycopy(cbuf, skipTo, cbuf, off, end - skipTo)
                    }
                    end = end - hasToRead
                    count = reader.read(cbuf, end, hasToRead)
                    if (count > 0) {
                        end += count
                    }
                }
            }
            if (cbuf[off] == NL) {
                atLineStart = true;
            }
            off++;
        }
        return end - start;
    }

    @Override
    void close() throws IOException {
        reader.close()
    }
}

