package com.thomsonreuters.lsps.transmart.files
/**
 * CelFilesConverter
 * Created by bondarev on 3/25/14.
 */
@Mixin(MetaInfoHeader)
class VcfFile extends CsvLikeFile {
    private Entry currentEntry = new Entry()
    private String[] _samples
    private Map<CharSequence, InfoField> infoFields
    private int firstSampleIndex

    VcfFile(File file) {
        super(file, '##')
    }

    static class SampleData {
        String allele1
        String allele2
        String alleleSeparator
    }

    static class InfoField {
        String id
        String description
        String type
        String number
    }

    class Entry {
        private String[] data
        private Map samplesData
        private Map<InfoField, Object[]> infoData
        private String[] alternatives

        void setData(data) {
            this.data = data
            this.samplesData = null
            this.infoData = null
            this.alternatives = (~/,/).split(alternativesString)
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

        String getAlternativesString() {
            data[4]
        }

        String getQual() {
            data[5]
        }

        String getFilter() {
            data[6]
        }

        String getInfoString() {
            data[7]
        }

        String getFormatString() {
            data[8]
        }

        String[] getSampleValues() {
            data[firstSampleIndex..-1]
        }

        String[] getAlternatives() {
            alternatives
        }

        Map<CharSequence, SampleData> getSamplesData() {
            samplesData ?: (samplesData = buildSamplesData())
        }

        Map<InfoField, Object> getInfoData() {
            infoData ?: (infoData = buildInfoData())
        }

        private Map<CharSequence, SampleData> buildSamplesData() {
            Map<CharSequence, SampleData> samplesData = [:]
            int gtIndex = formatString.split(':').toList().indexOf('GT')
            if (gtIndex != -1) {
                VcfFile.this.samples.eachWithIndex { sample, idx ->
                    CharSequence[] parts = data[firstSampleIndex + idx].split(':')
                    SampleData sampleData = new SampleData()
                    if (gtIndex != -1) {
                        def matches = parts[gtIndex] =~ /(\d+|\.)(?:([\/|])(\d+|\.))?/
                        if (matches) {
                            sampleData.allele1 = matches[0][1]
                            sampleData.alleleSeparator = matches[0][2]
                            sampleData.allele2 = matches[0][3]
                        }
                    }
                    samplesData[sample] = sampleData
                }
            }
            samplesData
        }

        private Map<InfoField, Object[]> buildInfoData() {
            infoString.split(';').collectEntries {
                def parts = it.split('=')
                [infoFields[parts[0]], parts[1].split(',')]
            }
        }
    }

    @Override
    protected void prepare() {
        super.prepare()
        int formatColumnIndex = header.findIndexOf('FORMAT'.&equals)
        if (formatColumnIndex == -1) throw new UnsupportedOperationException("Column FORMAT was not found in VCF file")
        firstSampleIndex = formatColumnIndex + 1
        _samples = header[firstSampleIndex..-1]
        infoFields = buildInfoFields()
    }

    String[] getSamples() {
        prepareIfRequired()
        return _samples
    }

    Map<CharSequence, InfoField> getInfoFields() {
        prepareIfRequired()
        infoFields
    }

    private Map<CharSequence, InfoField> buildInfoFields() {
        headComments.findAll { it.startsWith('INFO=') }.collectEntries { headComment ->
            String fieldDescription = (headComment =~ /^INFO=<(.*)>$/)[0][1]
            def initFields = [:]
            fieldDescription.eachMatch(/,?(\w+)=('[^']*'|"[^"]*"|[^,]*)/) {
                initFields[it[1].toLowerCase()] = it[2].charAt(0) == '\'' || it[2].charAt(0) == '\"' ? it[2][1..-2] : it[2]
            }
            [initFields.id, new InfoField(initFields)]
        }
    }

    @Override
    protected makeEntry(String line) {
        currentEntry.data = line.split('\t')
        return currentEntry
    }
}
