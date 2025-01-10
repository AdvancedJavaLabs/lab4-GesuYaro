package org.itmo

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class SaleMapper: Mapper<Any, Text, Text, Metrics>() {

    override fun map(key: Any, value: Text, context: Context) {
        val line = value.toString()
        if (line.startsWith("transaction_id")) return
        val splittedLine = line.split(",")
        if (splittedLine.size != 5) return
        val (transactionId, productId, category, price, quantity) = splittedLine
        context.write(
            category.toText(),
            Metrics(
                revenue = price.toDouble() * quantity.toInt(),
                quantity = quantity.toInt()
            )
        )
    }
}