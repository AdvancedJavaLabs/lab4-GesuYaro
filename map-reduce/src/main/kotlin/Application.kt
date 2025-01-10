package org.itmo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import kotlin.system.exitProcess

class Application(private val args: Array<String>) {
    fun start() {
        val mainJob: Job = Job.getInstance(Configuration(), "sales aggregator")
        with(mainJob) {
            setJarByClass(Application::class.java)
            mapperClass = SaleMapper::class.java
            reducerClass = SaleReducer::class.java
            mapOutputKeyClass = Text::class.java
            mapOutputValueClass = Metrics::class.java
            outputKeyClass = Text::class.java
            outputValueClass = Metrics::class.java
        }
        FileInputFormat.addInputPath(mainJob, Path(args[0]))
        FileOutputFormat.setOutputPath(mainJob, Path("/tmp_output"))
        if (!mainJob.waitForCompletion(true)) exitProcess(1)

        val sortJob: Job = Job.getInstance(Configuration(), "sales sorter")
        with(sortJob) {
            setJarByClass(Application::class.java)
            mapperClass = SortMapper::class.java
            reducerClass = SortReducer::class.java
            mapOutputKeyClass = DoubleWritable::class.java
            mapOutputValueClass = CategoryWithMetrics::class.java
            outputKeyClass = Text::class.java
            outputValueClass = Metrics::class.java
            setSortComparatorClass(SortComparator::class.java)
        }
        FileInputFormat.addInputPath(sortJob, Path("/tmp_output"))
        FileOutputFormat.setOutputPath(sortJob, Path(args[1]))
        exitProcess(if (sortJob.waitForCompletion(true)) 0 else 1)
    }
}