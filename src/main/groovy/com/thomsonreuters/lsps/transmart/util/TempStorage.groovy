package com.thomsonreuters.lsps.transmart.util

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter

/**
 * Date: 23.04.2015
 * Time: 11:56
 */
@Singleton(strict = false)
class TempStorage {
    private File tmpDir

    private TempStorage() {
        if (instance != null) {
            throw new RuntimeException("Can't instantiate singleton " + this.class.name +
                    ". Use " + this.class.name + ".instance")
        }
        tmpDir = new File("tmp")
        tmpDir.deleteDir()
        tmpDir.mkdirs()
        tmpDir.deleteOnExit()
    }

    /**
     * Creates singleton temp directory from provided template directory.
     * It will do nothing if temp directory with this name already exists.
     * If optional newName provided, then it uses provided parameter as directory name.
     * If optional block provided, then it will be called for newly created directory (if no directory exists)
     *
     * @param template - template directory
     * @param newName - name of singleton temp directory
     * @param block - initialization block
     * @return singleton temp directory file
     */
    public File createSingletonTempDirectoryFrom(
            File template, String newName=null,
            @ClosureParams(value=SimpleType.class, options = "java.io.File") Closure block=null) {
        File dstDir = new File(tmpDir, newName ?: template.name)
        if (!dstDir.exists()) {
            FileUtils.copyDirectory(template, dstDir)
            if (block != null) {
                block(dstDir)
            }
            FileUtils.listFilesAndDirs(dstDir, TrueFileFilter.TRUE, TrueFileFilter.TRUE)*.deleteOnExit()
        }
        return dstDir
    }

    public File createTempFile(String prefix, String suffix) {
        File file = File.createTempFile(prefix, suffix, this.tmpDir)
        file.setReadable(true, false);
        file.setWritable(true, false);
        file.setExecutable(true, false);
        file.deleteOnExit()
        return file
    }
}
