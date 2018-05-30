package com.contactsunny.lab.SparkS3FileRead;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkS3FileRead implements CommandLineRunner {

    private static final Logger logger = Logger.getLogger(SparkS3FileRead.class);

    private JavaSparkContext javaSparkContext;

    @Value("${s3.accessKey}")
    private String s3AccessKey;

    @Value("${s3.accessSecret}")
    private String s3AccessSecret;

    @Value("${s3.bucketName}")
    private String s3BucketName;

    @Value("${s3.filePath}")
    private String s3FilePath;

    @Value("${s3.protocol}")
    private String s3Protocol;

    public static void main( String[] args )
    {
        SpringApplication.run(SparkS3FileRead.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        /*
        We're going to create a new JavaSparkContext to work with Apache Spark.
        Spark always needs a context to work with, unless you're working with
        the Spark shell.
         */
        javaSparkContext = createJavaSparkContext();

        /*
        Calling the function which configures the Hadoop configuration of the Spark
        context, sets the AWS credentials, and reads the file.
         */
        readS3File();
    }

    /**
     * Function to set Spark's Hadoop configuration, AWS credentials, and read a text file
     * from Amazon S3. The function then counts the number of lines in the files, and
     * prints it to the console.
     */
    private void readS3File() {

        /*
        Creating the S3 path of the file using the configuration parameters.
         */
        String s3FilePath = this.s3Protocol + this.s3BucketName + "/" + this.s3FilePath;

        /*
        Getting the Hadoop configuration object from the JavaSparkContext object,
        and over-riding the default values from the configuration values.
         */
        Configuration hadoopConfig = this.javaSparkContext.hadoopConfiguration();
        hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");

        /*
        Setting the AWS credentials in the Hadoop configuration object.
         */
        hadoopConfig.set("fs.s3a.awsAccessKeyId", this.s3AccessKey);
        hadoopConfig.set("fs.s3a.awsSecretAccessKey", this.s3AccessSecret);

        /*
        Creating an RDD from the S3 file. The "lines" variable will have
        String RDDs, which are a list of all the lines in the file - each line
        in the file becomes a list item.
         */
        JavaRDD<String> lines = this.javaSparkContext.textFile(s3FilePath);

        /*
        Calling the "count()" action on the "lines" RDD to get the number of
        lines in the file.
         */
        long lineCount = lines.count();

        /*
        Printing the value to console.
         */
        logger.info("==========================================");
        logger.info("Line count: " + lineCount);
        logger.info("==========================================");
    }

    private JavaSparkContext createJavaSparkContext() {

        /*
        Create a SparkConf object and set the app name and the master.
        These two values could come from either the properties file, or the command line, or
        could be hard-coded, like I've done here.
        But the "host" parameter, if not local, has to be a valid Spark Master URL.
         */
        SparkConf conf = new SparkConf().setAppName("SparkS3FileRead").setMaster("local[1]");

        /*
        Using the SparkConf object created above, we're going to create a new JavaSparkContext object
        and return the same.
         */
        return new JavaSparkContext(conf);
    }
}
