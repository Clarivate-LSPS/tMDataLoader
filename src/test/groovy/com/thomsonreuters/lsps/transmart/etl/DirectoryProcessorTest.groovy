package com.thomsonreuters.lsps.transmart.etl

import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.model.ZipParameters
import spock.lang.Specification

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


class DirectoryProcessorTest extends Specification {

    private enum Mark {
        DONE("_DONE_"), FAIL("_FAIL_")

        private String value

        Mark(value) {
            this.value = value
        }
    }

    DirectoryProcessor directoryProcessor
    def config = [logger : new Logger()]

    def "Directory processor must successfully process study folder "() {
        setup:
            GroovySpy(DataProcessorFactory, global: true)
            DataProcessorFactory.newDataProcessor(*_) >> {
                AbstractDataProcessor dataProcessor = Mock(AbstractDataProcessor)
                dataProcessor.process(*_) >> true
                dataProcessor
            }

            directoryProcessor = new DirectoryProcessor(config)

            File etlDir = new File('fixtures', 'Test Directory Processor')
            File studyFolder = createTestStudyFolder(new File(etlDir ,'Test Studies/Test Folder Study'))
            createZipFile(studyFolder)
        when:
            boolean successfullyProcessed = directoryProcessor.process(etlDir.absolutePath)
        then:
            successfullyProcessed
            allStudyFolderMarking(Paths.get('fixtures', 'Test Directory Processor/Test Studies'), Mark.DONE)
        cleanup:
            deleteTestDirectory(etlDir)
    }

    private boolean allStudyFolderMarking(Path dir, Mark mark) {
        for (Path study : Files.newDirectoryStream(dir)) {
            if (Files.notExists(study))
                continue

            if ( !study.getFileName().toString().startsWith(mark.value) ) {
                return false
            }

            if (study.fileName.toString().endsWith('.zip')) {
                study = FileSystems.newFileSystem(study, null).
                        getPath("${study.fileName.toString().replace('.zip', '')}")
            }


            Collection studyDataWithoutMark = Files.newDirectoryStream(study).withCloseable {
                it.findAll { Path studyData ->
                    !studyData.getFileName().toString().startsWith(mark.value)
                }
            }
            if (!studyDataWithoutMark.empty) {
                return false
            }
        }

        return true
    }

    private File createTestStudyFolder(File studyFolder) {
        studyFolder.mkdirs()

        new File(studyFolder,'ACGHDataToUpload').mkdir()
        new File(studyFolder,'ClinicalDataToUpload').mkdir()
        new File(studyFolder,'ExpressionDataToUpload').mkdir()
        new File(studyFolder,'MetaDataToUpload').mkdir()
        new File(studyFolder,'SNPDataToUpload').mkdir()

        studyFolder
    }

    private File createZipFile(File sourceDir) {
        File result = new File(sourceDir.parent, "${sourceDir.name}.zip")
        ZipFile zipFile = new ZipFile(result)
        zipFile.createZipFileFromFolder(sourceDir, new ZipParameters(), false, 0)
        result
    }

    private deleteTestDirectory(File dir) {
        for (File child : dir.listFiles()) {
            if (child.isDirectory())
                deleteTestDirectory(child)
            else
                child.delete()
        }

        dir.delete()
    }

}
