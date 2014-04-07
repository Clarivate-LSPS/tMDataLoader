package com.thomsonreuters.lsps.transmart.files
/**
 * CelFilesConverter
 * Created by bondarev on 3/25/14.
 */
@Mixin(MetaInfoHeader)
class VcfFile extends CsvLikeFile {
    private Entry currentEntry = new Entry();
    private String[] _samples;

    VcfFile(File file) {
        super(file, '##')
    }

    static class Entry {
        private String[] data
        private Map calls

        void setData(data) {
            this.data = data
            this.calls = null
        }

        int getChromosome() {
            data[0] as int
        }

        long getChromosomePosition() {
            data[1] as long
        }

        String getProbesetId() {
            data[2]
        }

        private Map buildCalls() {
            return [:]
        }

        Map getCalls() {
            calls ?: (calls = buildCalls())
        }
    }

    String[] getSamples() {
        if (!_samples) {
            int formatColumnIndex = header.findIndexOf('FORMAT'.&equals)
            if (formatColumnIndex == -1) throw new UnsupportedOperationException("Column FORMAT was not found in VCF file")
            _samples = header[formatColumnIndex + 1..-1]
        }
        return _samples
    }

    @Override
    protected makeEntry(String line) {
        currentEntry.data = line.split('\t')
        return currentEntry
    }
}
