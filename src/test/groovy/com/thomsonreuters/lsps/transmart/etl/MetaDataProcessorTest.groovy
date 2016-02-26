package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.db.sql.SqlMethods

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertThat

class MetaDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MetaDataProcessor _processor;

    String root = 'Test Studies'
    String studyName = 'Test Study'
    String studyId = 'GSE0'

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        Study.deleteById(config, studyId)
        Study.deleteStudyMetaDataById(studyId, sql)
        Fixtures.clinicalData.load(config)
        runScript('i2b2_load_study_metadata.sql')
    }

    MetaDataProcessor getProcessor() {
        _processor ?: (_processor = new MetaDataProcessor(config))
    }

    void testItLoadsData() {
        withErrorLogging {
            processor.process(new File(studyDir(studyName, studyId), "MetaDataToUpload").toPath(),
                    [name: studyName, node: "Test Studies\\${studyName}_${studyId}".toString()])
        }

        use(SqlMethods) {
            def studyMetaData = db.findRecord('tm_lz.lt_src_study_metadata',
                    study_id: studyId)

            def experimentId = db.findRecord('biomart.bio_experiment',
                    accession : studyId)?.'bio_experiment_id'

            def bioCompoundId = db.findRecord('biomart.bio_compound',
                    generic_name: studyMetaData.compound)?.'bio_compound_id'

            def bioDiseaseId = db.findRecord('biomart.bio_disease',
                    disease: studyMetaData.disease)?.'bio_disease_id'

            def bioTaxonomyId = db.findRecord('biomart.bio_taxonomy',
                    taxon_name: studyMetaData.organism)?.'bio_taxonomy_id'

            def NCBIRepositoryId = db.findRecord('biomart.bio_content_repository',
                    location: 'http://www.ncbi.nlm.nih.gov/', active_y_n: 'Y',
                    repository_type: 'NCBI', location_type: 'URL')?.bio_content_repo_id

            assertNotNull('Experiment not exist' ,experimentId)
            assertThat(db, hasRecord('biomart.bio_experiment', [bio_experiment_id : experimentId],
                    [design : 'STUDY_DESIGN:INTERVENTIONAL',
                     biomarker_type: 'STUDY_BIOMARKER_TYPE:EFFICACY_BIOMARKER',
                     access_type : 'STUDY_ACCESS_TYPE:COMMERCIAL',
                     institution : 'STUDY_INSTITUTION:TEST_INSTITUTION']))
            assertThat(db, hasRecord('biomart.bio_data_uid', [bio_data_id: experimentId], [:]))

            assertNotNull('Compound load fail', bioCompoundId)
            assertThat(db, hasRecord('biomart.bio_data_compound', [bio_compound_id: bioCompoundId],
                    [etl_source: "METADATA:${studyId}"]))

            assertNotNull('Disease load fail', bioDiseaseId)
            assertThat(db, hasRecord('biomart.bio_data_disease', [bio_disease_id: bioDiseaseId],
                    [etl_source: "METADATA:${studyId}"]))

            assertNotNull('Organism type load fail', bioTaxonomyId)
            assertThat(db, hasRecord('biomart.bio_data_taxonomy', [bio_taxonomy_id: bioTaxonomyId],
                    [etl_source: "METADATA:${studyId}"]))

            assertNotNull('NCBI content repository not exist', NCBIRepositoryId)
            assertThat(db, hasRecord('biomart.bio_content', [location: "geo/query/acc.cgi?acc=${studyId}"],
                    [repository_id : NCBIRepositoryId]))

            assertThat(db, hasRecord('i2b2metadata.i2b2_tags',
                    [path : "\\${root}\\${studyName}\\", tag_type : 'Trial'], [tag : "${studyId}"]))
            assertThat(db, hasRecord('i2b2metadata.i2b2_tags',
                    [path : "\\${root}\\${studyName}\\", tag_type : 'Compound'], [tag : "${studyMetaData.compound}"]))
            assertThat(db, hasRecord('i2b2metadata.i2b2_tags',
                    [path : "\\${root}\\${studyName}\\", tag_type : 'Disease'], [tag : "${studyMetaData.disease}"]))

        }

    }

    void testCreateStudyFolder() {
        withErrorLogging {
            processor.process(new File(studyDir(studyName, studyId), "MetaDataToUpload").toPath(),
                    [name: studyName, node: "Test Studies\\${studyName}_${studyId}".toString()])
        }

        use(SqlMethods) {
            def experimentId = db.findRecord('biomart.bio_experiment',
                    accession : studyId)?.'bio_experiment_id'
            assertNotNull('Experiment not exist', experimentId)

            def bioDataUniqueId = db.findRecord('biomart.bio_data_uid', bio_data_id : experimentId)?.'unique_id'
            assertNotNull('Data uid not exist', bioDataUniqueId)

            def etlProgramId = db.findRecord('fmapp.fm_folder', folder_name: 'etl-program',
                    folder_type: 'PROGRAM',  folder_level: 0)?.'folder_id';
            assertNotNull('Etl program folder not exist', etlProgramId)

            def studyFolderId = db.findRecord('fmapp.fm_folder', folder_name: 'GSE0',
                    folder_type: 'STUDY',  folder_level: 1, parent_id: etlProgramId)?.'folder_id';
            assertNotNull('Study folder not exist', studyFolderId)

            assertThat(db, hasRecord('amapp.am_tag_association', [subject_uid : "FOL:${studyFolderId}"],
                    [object_uid: 'STUDY_PHASE:DEVELOPMENT_CANDIDATE']))

            assertThat(db, hasRecord('fmapp.fm_folder_association', [folder_id: studyFolderId, object_uid : bioDataUniqueId], [:]))
        }
    }
}
