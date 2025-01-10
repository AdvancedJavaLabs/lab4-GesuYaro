package org.itmo

import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput

data class Metrics(
    var revenue: Double = 0.0,
    var quantity: Int = 0
): Writable {

    override fun write(out: DataOutput) {
        out.writeDouble(revenue)
        out.writeInt(quantity)
    }

    override fun readFields(`in`: DataInput) {
        revenue = `in`.readDouble()
        quantity = `in`.readInt()
    }

    override fun toString(): String {
        val revenueFormatted = revenue.toBigDecimal().toPlainString()
        return "$revenueFormatted,$quantity"
    }


}
