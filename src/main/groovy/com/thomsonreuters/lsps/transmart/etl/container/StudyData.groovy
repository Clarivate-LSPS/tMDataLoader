package com.thomsonreuters.lsps.transmart.etl.container

import java.nio.file.Files
import java.nio.file.Path
import java.security.InvalidParameterException

class StudyData {

    Path studyDataPath

    StudyData(Path studyDataPath) {
        if (!Files.isDirectory(studyDataPath))
            throw new InvalidParameterException("Study path: " +
                    "${studyDataPath.toString()} not directory")

        this.studyDataPath = studyDataPath
    }

    void eachFile(Closure closure) {
        Files.newDirectoryStream(studyDataPath).withCloseable { paths ->

            paths.each { Path path ->
                if (Files.isDirectory(path))
                    return

                Files.newBufferedReader(path).withReader {
                    closure.call(path.getFileName().toString(), it)
                }
            }

        }
    }

    void writeFile(String fileName, Closure closure) {
       if (!fileName)
           throw new InvalidParameterException('File name is empty')

       Files.newBufferedWriter(studyDataPath.resolve(fileName)).withWriter {
            closure.call(it)
       }
    }

 }
