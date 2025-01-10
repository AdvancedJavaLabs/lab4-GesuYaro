package org.itmo

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput

data class CategoryWithMetrics(
    val category: Text = Text(),
    val metrics: Metrics = Metrics()
): Writable {

    override fun write(out: DataOutput) {
        category.write(out)
        metrics.write(out)
    }

    override fun readFields(`in`: DataInput) {
        category.readFields(`in`)
        metrics.readFields(`in`)
    }

}
