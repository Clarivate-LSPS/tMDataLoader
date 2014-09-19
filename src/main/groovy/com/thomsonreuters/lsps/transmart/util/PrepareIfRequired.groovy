package com.thomsonreuters.lsps.transmart.util

/**
 * Date: 19.09.2014
 * Time: 13:05
 */
@Category(Object)
class PrepareIfRequired {
    private boolean _prepared

    public final boolean isPrepared() {
        return this._prepared
    }

    public final void prepareIfRequired() {
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

    protected void prepare() {}
}
