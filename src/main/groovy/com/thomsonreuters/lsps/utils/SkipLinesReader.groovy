package com.thomsonreuters.lsps.utils

import groovy.transform.CompileStatic

/**
 * Date: 11.09.2014
 * Time: 17:12
 */

@CompileStatic
public class SkipLinesReader extends Reader {
    private Reader reader
    private boolean atLineStart = true;
    private char[] prefixBuffer
    private int skippedLinesCount

    private static final char NL = '\n';
    private static final char[] EMPTY_BUFFER = new char[0]
    private TreeSet<String> prefixes = new TreeSet<>()

    SkipLinesReader(Reader reader, Collection<String> skipPrefixes) {
        if (!reader.markSupported()) {
            reader = new BufferedReader(reader)
        }
        this.reader = reader
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
    boolean ready() throws IOException {
        return this.reader.ready()
    }

    @Override
    int read() throws IOException {
        if (atLineStart) {
            skipLines();
        }
        int c = reader.read();
        atLineStart = c == (int) NL;
        return c;
    }

    private int skipToNextLine(char[] buf, int off, int end) {
        skippedLinesCount++
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
        System.arraycopy(buf, off, prefixBuffer, 0, prefixSize)
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
            int prefixSize = readPrefixBuffer(buf, off, end);
            prefixes:
            for (String prefix : prefixes) {
                if (prefix.length() > prefixSize) {
                    break;
                }
                for (int i = 0; i < prefix.length(); i++) {
                    if (prefix.charAt(i) != prefixBuffer[i]) {
                        continue prefixes;
                    }
                }
                off = skipToNextLine(buf, Math.min(off + prefixSize, end), end)
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
            atLineStart = cbuf[off] == NL
            off++;
        }
        return end - start;
    }

    @Override
    void close() throws IOException {
        reader.close()
    }
}
