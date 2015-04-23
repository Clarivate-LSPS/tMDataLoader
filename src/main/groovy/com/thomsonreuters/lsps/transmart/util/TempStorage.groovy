package com.thomsonreuters.lsps.transmart.util

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

    public File createTempFile(String prefix, String suffix) {
        File file = File.createTempFile(prefix, suffix, tmpDir)
        file.setReadable(true, false);
        file.setWritable(true, false);
        file.setExecutable(true, false);
        file.deleteOnExit()
        return file
    }
}
