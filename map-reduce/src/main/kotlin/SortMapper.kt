package org.itmo

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper


class SortMapper: Mapper<Any, Text, DoubleWritable, CategoryWithMetrics>() {
    override fun map(key: Any, value: Text, context: Context) {
        val (category, revenue, quantity) = value.toString().split("\t", ",")

        context.write(
            revenue.toDoubleWritable(),
            CategoryWithMetrics(
                category = category.toText(),
                metrics = Metrics(
                    revenue = revenue.toDouble(),
                    quantity = quantity.toInt()
                )
            )
        )
    }

}