package com.thomsonreuters.lsps.transmart.util

import java.nio.file.Files
import java.nio.file.Path

class PathUtils {

    /**
     * Recursive move
     * @param source
     * @param target
     * @return
     */
    static def movePath(Path source, Path target) {
        Files.newDirectoryStream(source).each { Path childPath ->
            Path newTarget = target.resolve("${childPath.fileName}")
            Files.move(childPath, newTarget)

            if (Files.isDirectory(childPath)) {
                movePath(childPath, newTarget)
            }
        }
    }

    /**
     * Recursive delete
     * @param dir
     * @return
     */
    static def deletePath(Path dir) {
        Files.newDirectoryStream(dir).each {
            if (Files.isDirectory(it)) {
                deletePath(it)
            } else {
                Files.delete(it)
            }
        }

        Files.delete(dir)
    }
}
