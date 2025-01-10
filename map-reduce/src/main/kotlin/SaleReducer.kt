package org.itmo

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class SaleReducer: Reducer<Text, Metrics, Text, Metrics>() {

    override fun reduce(key: Text, values: MutableIterable<Metrics>, context: Context) {
        val (revenue, quantity) = values
            .reduce { pair1, pair2 ->
                Metrics(
                    revenue = pair1.revenue + pair2.revenue,
                    quantity = pair1.quantity + pair2.quantity
                )
            }
        context.write(
            key,
            Metrics(
                revenue = revenue,
                quantity = quantity
            )
        )
    }
}