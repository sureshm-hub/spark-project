# setup Hadoop Bin path:
    Spark can run in “local mode” without Hadoop. 
    It’ll use your local filesystem (file:///) for reads/writes.
    You can load data like: spark.read().csv("src/main/resources/sample-data/employees.csv"
    For local IntelliJ Spark practice (using SparkSession.builder().master("local[*]")), you do not need to set up Hadoop or its bin/ path at all.

# When You Do Need Hadoop bin/ (HADOOP_HOME)
    You need Hadoop binaries only if you:
        Use HDFS paths like hdfs://namenode:9000/path/to/file
        Use Spark in cluster mode (YARN, EMR, etc.)
        Want to write Parquet/ORC with compression codecs that depend on Hadoop libs
    And on Windows:
        Download WinUtils.exe
        Place it under C:\hadoop\bin\winutils.exe
        Set environment variable: HADOOP_HOME=C:\hadoop

# When running Spark on a newer JDK (17/21).
    Spark (or one of its deps) is still trying to use internal sun.* classes, with the Java module system, those internals are blocked unless you explicitly open/export them.
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
        --add-opens=java.base/java.nio=ALL-UNNAMED