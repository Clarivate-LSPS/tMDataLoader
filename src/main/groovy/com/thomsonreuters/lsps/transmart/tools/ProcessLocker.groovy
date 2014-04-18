package com.thomsonreuters.lsps.transmart.tools

/**
 * Created by bondarev on 4/1/14.
 */
class ProcessLocker {
    private File lockFile
    private boolean locked

    private ProcessLocker(String name) {
        File tmpDir = new File(System.getProperty('java.io.tmpdir'))
        tmpDir.mkdirs()
        this.lockFile = new File(tmpDir, ".${name}.process-locker.lock")
    }

    static ProcessLocker get(String name) {
        return new ProcessLocker(name)
    }

    public File getLockFile() {
        return lockFile
    }

    boolean tryLock() {
        if (isLocked()) {
            return false
        }
        lock()
        return true
    }

    boolean isLocked() {
        lockFile.exists()
    }

    void lock() {
        if (isLocked()) throw new RuntimeException('Process is already locked')
        lockFile.createNewFile()
        lockFile.deleteOnExit()
        locked = true
    }

    void unlock() {
        if (!locked) throw new RuntimeException('Can\'t unlock file not locked by this process')
        lockFile.delete()
    }
}
