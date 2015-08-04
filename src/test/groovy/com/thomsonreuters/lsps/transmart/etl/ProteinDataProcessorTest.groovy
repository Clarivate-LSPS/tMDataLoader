package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ProteinData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.sql.DatabaseType

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

class ProteinDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    ProteinData proteinData = Fixtures.proteinData
    String studyName = proteinData.studyName
    String studyId = proteinData.studyId

    String platformId = 'RBM999'

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_LOAD_PROTEOMICS_ANNOT.sql')
        if (database?.databaseType == DatabaseType.Postgres) {
            runScript('I2B2_PROCESS_PROTEOMICS_DATA.sql')
        }
    }

    void assertThatSampleIsPresent(String sampleId, sampleData, currentStudyId, currentPlatformId, String prop = 'zscore') {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ? and platform = ?',
                currentStudyId, sampleId, 'PROTEIN')
        assertThat(sample, notNullValue())

        sampleData.each { gene_symbol, value ->
            def rows = sql.rows("select d.zscore, d.log_intensity from deapp.de_subject_protein_data d " +
                    "inner join deapp.de_protein_annotation a on d.protein_annotation_id = a.id " +
                    "where a.gpl_id = ? and d.assay_id = ? and d.gene_symbol = ?",
                    currentPlatformId, sample.assay_id, gene_symbol)
            assertThat(rows?.size(), equalTo(1))
            assertEquals(value as double, rows[0][prop] as double, 0.001)
        }
    }

    void testItLoadsData() {
        proteinData.load(config)
        assertThat(db, hasSample(studyId, 'P50440'))
        assertThat(db, hasPatient('GSM918945').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test Protein Platform\\").
                withPatientCount(5))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'GSM918944', sample_cd: 'P50440'],
                [platform: 'PROTEIN']))

        assertThat(db, hasRecord('deapp.de_subject_protein_data',
                [trial_name: studyId, subject_id: 'GSM918946', gene_symbol: 'P50440'],
                [component: 'RPPGFSPFR(QTF-2)']))

        assertThatSampleIsPresent('P50440', ['O00231': 0.02146], studyId, platformId)
    }

    void testItMergeSamples() {
        Study.deleteById(config, studyId)

        proteinData.load(config)
        assertThatSampleIsPresent('O00231', ['P50440': 22.6096], studyId, platformId, 'log_intensity')

        Fixtures.additionalProteinData.load(config)

        assertThat(db, hasSample(studyId, 'P50440', platform: 'PROTEIN', subject_id: 'GSM918944', gpl_id: platformId))
        assertThat(db, hasSample(studyId, 'Q50440', platform: 'PROTEIN', subject_id: 'GSM818944', gpl_id: platformId))
        assertThat(db, hasPatient('GSM918945').inTrial(studyId))
        assertThat(db, hasPatient('GSM818945').inTrial(studyId))

        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test Protein Platform\\").
                withPatientCount(4))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Additional\\Test Protein Platform\\").
                withPatientCount(5))

        assertThatSampleIsPresent('P50440', ['O00231': 22.0020], studyId, platformId, 'log_intensity')
        assertThatSampleIsPresent('Q50440', ['O00231': 22.0020], studyId, platformId, 'log_intensity')
        assertThatSampleIsPresent('P50440', ['P50440': 21.7796], studyId, platformId, 'log_intensity')
        assertThatSampleIsPresent('Q50440', ['P50440': 22.0020], studyId, platformId, 'log_intensity')
        // test modified sample
        assertThatSampleIsPresent('O00231', ['P50440': 22.0020], studyId, platformId, 'log_intensity')
    }

    void testItLoadsDataWithoutPeptide() {
        def proteinDataWithoutPeptide = Fixtures.proteinDataWithoutPeptide
        def studyIdWithoutPeptide = proteinDataWithoutPeptide.studyId
        def studyNameWithoutPeptide = proteinDataWithoutPeptide.studyName
        String platformIdWithoutPeptide = 'RBM888'
        proteinDataWithoutPeptide.load(config)

        assertThat(db, hasSample(studyIdWithoutPeptide, 'P504401'))
        assertThat(db, hasPatient('GSM9189451').inTrial(studyIdWithoutPeptide))
        assertThat(db, hasNode("\\Test Studies\\${studyNameWithoutPeptide}\\Biomarker Data\\Test Protein Platform 2\\").
                withPatientCount(5))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyIdWithoutPeptide, gpl_id: platformIdWithoutPeptide, subject_id: 'GSM9189441', sample_cd: 'P504401'],
                [platform: 'PROTEIN']))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyIdWithoutPeptide, gpl_id: platformIdWithoutPeptide, subject_id: 'GSM9189441', sample_cd: 'P504401'],
                [platform: 'PROTEIN']))

        assertThat(db, hasRecord('deapp.de_subject_protein_data',
                [trial_name: studyIdWithoutPeptide, subject_id: 'GSM9189461', gene_symbol: 'P026471'],
                [component: 'P026471']))

        assertThatSampleIsPresent('P504401', ['O002311': 0.02146], studyIdWithoutPeptide, platformIdWithoutPeptide)
    }
}
