package org.itmo

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.WritableComparator

class SortComparator: WritableComparator(DoubleWritable::class.java, true) {
    override fun compare(a: WritableComparable<*>?, b: WritableComparable<*>?): Int {
        return super.compare(b, a)
    }

    override fun compare(a: Any?, b: Any?): Int {
        return super.compare(b, a)
    }
}