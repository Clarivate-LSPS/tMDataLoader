package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 06.10.2014
 * Time: 18:17
 */
class Factor {
    Map<String, Long> counts;

    Factor() {
        this.counts = [:]
    }

    Factor(Map<String, Long> counts) {
        this.counts = counts
    }

    void addValue(String value) {
        Long n = counts.get(value)
        n = n.is(null) ? 1 : n + 1
        counts.put(value, n)
    }

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        Factor factor = (Factor) o

        if (counts != factor.counts) return false

        return true
    }

    int hashCode() {
        return (counts != null ? counts.hashCode() : 0)
    }

    @Override
    String toString() {
        return counts.toString()
    }
}
