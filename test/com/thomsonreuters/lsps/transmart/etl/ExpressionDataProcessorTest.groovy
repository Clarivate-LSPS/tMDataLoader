package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class ExpressionDataProcessorTest extends GroovyTestCase {
    def connectionSettings = [
            jdbcConnectionString: 'jdbc:postgresql:transmart',
            username            : 'postgres',
            password            : 'postgres',
            jdbcDriver          : 'org.postgresql.Driver'
    ]

    private Sql _sql
    private ExpressionDataProcessor _processor

    String studyName = 'TestSample'
    String studyId = 'GSE0'

    Sql getSql() {
        return _sql ?: (_sql = Sql.newInstance(connectionSettings.jdbcConnectionString,
                connectionSettings.password, connectionSettings.username,
                connectionSettings.jdbcDriver))
    }

    ExpressionDataProcessor getProcessor() {
        _processor ?: (_processor = new ExpressionDataProcessor([
                logger        : new Logger([isInteractiveMode: true]),
                db            : connectionSettings,
                controlSchema : 'tm_cz',
                securitySymbol: 'N'
        ]))
    }

    void assertThatSampleIsPresent(String sampleId) {
        def sample = sql.firstRow("select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?",
                studyId, sampleId)
        assertThat(sample, notNullValue())
    }

    void testItLoadsData() {
        processor.process(
                new File("Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Public Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('GSM1000000719')
    }

    void testItMergeSamples() {
        processor.process(
                new File("Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Public Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('GSM1000000719')
        processor.process(
                new File("Additional Samples/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Public Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('GSM2000000719')
        assertThatSampleIsPresent('GSM1000000719')
    }
}
