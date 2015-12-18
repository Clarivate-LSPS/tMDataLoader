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
                DataProcessor dataProcessor = Mock(DataProcessor)
                dataProcessor.process(*_) >> true
                dataProcessor
            }

            directoryProcessor = new DirectoryProcessor(config)
            File zip = createZipFile(new File('fixtures', 'Test Directory Processor/Test Studies/Test Folder Study'))
        when:
            boolean result = directoryProcessor.process(new File('fixtures', 'Test Directory Processor'))
        then:
            result
            allStudyFolderMarking(Paths.get('fixtures', 'Test Directory Processor/Test Studies'), Mark.DONE)
        cleanup:
            deleteStudyMarkers(Paths.get('fixtures', 'Test Directory Processor/Test Studies'), Mark.DONE)
            File markZipFile = new File(zip.parentFile, "${Mark.DONE.value}${zip.name}")
            markZipFile.delete()
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

    private def deleteStudyMarkers(Path dir, Mark mark) {
        Files.newDirectoryStream(dir).withCloseable {
            it.each { Path study ->

                if (study.fileName.toString().endsWith('.zip')) {
                    return
                }

                Files.newDirectoryStream(study).withCloseable {
                    it.each { Path studyData ->
                        Files.move(studyData, studyData.resolveSibling(studyData.fileName.toString().replace(mark.value, '')))
                    }
                }

                Files.move(study, study.resolveSibling(study.fileName.toString().replace(mark.value, '')))
            }
        }

    }

    private File createZipFile(File sourceDir) {
        File result = new File(sourceDir.parent, "${sourceDir.name}.zip")
        ZipFile zipFile = new ZipFile(result)
        zipFile.createZipFileFromFolder(sourceDir, new ZipParameters(), false, 0)
        result
    }


}
