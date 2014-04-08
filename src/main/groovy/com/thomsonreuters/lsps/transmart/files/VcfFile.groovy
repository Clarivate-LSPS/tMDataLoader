package com.thomsonreuters.lsps.transmart.files
/**
 * CelFilesConverter
 * Created by bondarev on 3/25/14.
 */
@Mixin(MetaInfoHeader)
class VcfFile extends CsvLikeFile {
    private Entry currentEntry = new Entry()
    private String[] _samples
    private int formatColumnIndex
    private int firstSampleIndex

    VcfFile(File file) {
        super(file, '##')
    }

    static class SampleData {
        String allele1
        String allele2
        String alleleSeparator
    }

    class Entry {
        private String[] data
        private Map samplesData
        private String[] alternatives

        void setData(data) {
            this.data = data
            this.samplesData = null
            this.alternatives = (~/,/).split(data[4])
        }

        CharSequence getChromosome() {
            data[0]
        }

        long getChromosomePosition() {
            data[1] as long
        }

        String getProbesetId() {
            data[2]
        }

        String getReference() {
            data[3]
        }

        String[] getAlternatives() {
            alternatives
        }

        private Map buildSamplesData() {
            Map samplesData = [:]
            int gtIndex = data[formatColumnIndex].split(':').toList().indexOf('GT')
            VcfFile.this.samples.eachWithIndex { sample, idx ->
                CharSequence[] parts = data[firstSampleIndex + idx].split(':')
                SampleData sampleData = new SampleData()
                if (gtIndex != -1) {
                    def matches = parts[gtIndex] =~ /(.+)([\/|])(.+)/
                    if (matches) {
                        sampleData.allele1 = matches[0][1]
                        sampleData.alleleSeparator = matches[0][2]
                        sampleData.allele2 = matches[0][3]
                    }
                }
                samplesData[sample] = sampleData
            }
            samplesData
        }

        Map getSamplesData() {
            samplesData ?: (samplesData = buildSamplesData())
        }
    }

    @Override
    protected void prepare() {
        super.prepare()
        formatColumnIndex = header.findIndexOf('FORMAT'.&equals)
        if (formatColumnIndex == -1) throw new UnsupportedOperationException("Column FORMAT was not found in VCF file")
        firstSampleIndex = formatColumnIndex + 1
        _samples = header[firstSampleIndex..-1]
    }

    String[] getSamples() {
        prepareIfRequired()
        return _samples
    }

    @Override
    protected makeEntry(String line) {
        currentEntry.data = line.split('\t')
        return currentEntry
    }
}
