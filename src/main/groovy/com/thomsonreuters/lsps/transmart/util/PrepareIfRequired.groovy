package com.thomsonreuters.lsps.transmart.util

/**
 * Date: 19.09.2014
 * Time: 13:05
 */
trait PrepareIfRequired {
    private boolean _prepared

    public boolean isPrepared() {
        return this._prepared
    }

    public void prepareIfRequired() {
        if (!this._prepared) {
            this._prepared = true
            try {
                prepare()
            } catch (Throwable ex) {
                this._prepared = false
                throw ex;
            }
        }
    }

    void prepare() {}
}
