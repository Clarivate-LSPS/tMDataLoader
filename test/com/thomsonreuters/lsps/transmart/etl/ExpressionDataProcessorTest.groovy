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
                ]
        ])
        processor.process(
                new File("/home/transmart/data/Public Studies/Big_Test_GSE666/ExpressionDataToUpload"),
                [:])
    }
}
