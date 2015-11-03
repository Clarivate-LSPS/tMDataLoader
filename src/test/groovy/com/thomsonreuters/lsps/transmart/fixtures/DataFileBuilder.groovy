package com.thomsonreuters.lsps.transmart.fixtures

/**
 * Date: 03.11.2015
 * Time: 13:38
 */
class DataFileBuilder {
    final String studyId
    private String subjectId
    private List<List<String>> rows
    private List<String> columns

    DataFileBuilder(String studyId, List<String> columns) {
        this.studyId = studyId
        this.rows = []
        this.columns = columns
    }

    static build(String studyId, List<String> columns, Closure block) {
        def builder = new DataFileBuilder(studyId, columns)
        block.delegate = builder
        block.call()
        return builder
    }

    def forSubject(String subjectId, Closure block) {
        this.subjectId = subjectId
        try {
            block.delegate = this
            block.call()
        } finally {
            this.subjectId = null
        }
    }

    def row(String... values) {
        List row = [studyId]
        if (subjectId) {
            row.add(subjectId)
        }
        row.addAll(values)
        this.rows.add(row)
    }

    def row(Map<String, String> values) {
        List row = [studyId]
        if (subjectId) {
            row.add(subjectId)
        } else {
            row.add(values.SUBJ_ID)
        }
        columns.each {
            row.add(values[it])
        }
        this.rows.add(row)
    }

    void writeTo(PrintWriter writer) {
        writer.println((['STUDY_ID', 'SUBJ_ID'] + columns).join('\t'))
        for (def row : rows) {
            writer.println(row.join('\t'))
        }
    }
}
