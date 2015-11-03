package com.thomsonreuters.lsps.transmart.fixtures

/**
 * Date: 03.11.2015
 * Time: 13:38
 */
class MappingFileBuilder {
    private String dataFileName
    private List<List<String>> mappings = []

    def forDataFile(String dataFileName, Closure block) {
        this.dataFileName = dataFileName
        try {
            mapSpecial('STUDY_ID', 1)
            mapSpecial('SUBJ_ID', 2)
            block.delegate = this
            block.call()
        } finally {
            this.dataFileName = null
        }
    }

    def map(String categoryCd, int column, String label) {
        mappings.add([dataFileName, categoryCd, column.toString(), label, ''])
    }

    def mapLabelSource(String categoryCd, int column, String labelSource) {
        mappings.add([dataFileName, categoryCd, column.toString(), '\\', labelSource])
    }

    def mapSpecial(String name, int column) {
        mappings.add([dataFileName, '', column.toString(), name, ''])
    }

    static build(Closure block) {
        MappingFileBuilder builder = new MappingFileBuilder()
        block.delegate = builder
        block.call()
        return builder
    }

    void writeTo(PrintWriter writer) {
        writer.println(['filename', 'category_cd', 'col_nbr', 'data_label', 'data_label_source'].join('\t'))
        for (def mapping : mappings) {
            writer.println(mapping.join('\t'))
        }
    }
}
