package com.thomsonreuters.lsps.transmart.etl
/**
 * Created by bondarev on 2/24/14.
 */
class ExpressionDataProcessorTest extends GroovyTestCase {
    void testProcessFiles() {
        def processor = new ExpressionDataProcessor([
                logger: new Logger([isInteractiveMode: true]),
                db: [
                        jdbcConnectionString: 'jdbc:postgresql:transmart',
                        username: 'postgres',
                        password: 'postgres',
                        jdbcDriver: 'org.postgresql.Driver'
                ],
                controlSchema: 'tm_cz',
                securitySymbol: 'N'
        ])
        processor.process(
                new File("Public Studies/TestSample_GSE0/ExpressionDataToUpload"),
                [name: 'TestSample', node: 'Public Studies\\TestSample'])
    }
}
