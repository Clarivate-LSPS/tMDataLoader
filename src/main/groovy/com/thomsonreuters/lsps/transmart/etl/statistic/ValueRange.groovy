package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 09.10.2014
 * Time: 18:27
 */
class ValueRange<T extends Comparable<T>> {
    T from
    T to
    boolean includeFrom = true
    boolean includeTo = true

    boolean contains(T value) {
        boolean contained = true
        if (!from.is(null)) {
            contained = includeFrom ? value.compareTo(from) >= 0 : value.compareTo(from) > 0
        }
        if (!to.is(null)) {
            contained = includeTo ? value.compareTo(to) <= 0 : value.compareTo(to) < 0
        }
        return contained
    }
}
