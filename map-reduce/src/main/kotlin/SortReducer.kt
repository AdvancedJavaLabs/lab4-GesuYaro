package org.itmo

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer


class SortReducer: Reducer<DoubleWritable, CategoryWithMetrics, Text, Metrics>() {

    override fun reduce(key: DoubleWritable, values: Iterable<CategoryWithMetrics>, context: Context) {
        values.forEach { context.write(it.category, it.metrics) }
    }
}