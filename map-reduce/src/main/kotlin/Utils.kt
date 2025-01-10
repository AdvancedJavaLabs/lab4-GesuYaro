package org.itmo

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

fun Any.toText() = Text(toString())
fun Int.toIntWritable() = IntWritable(this)
fun String.toIntWritable() = toInt().toIntWritable()
fun Double.toDoubleWritable() = DoubleWritable(this)
fun String.toDoubleWritable() = toDouble().toDoubleWritable()