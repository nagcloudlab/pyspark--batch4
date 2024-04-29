
--------------------------------
Install java 8 or 11
---------------------------------

On Linux

```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

set JAVA_HOME environment variable ( optional )

```bash
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
```

Verify java installation

```bash
java -version
```

---

On Windows 
download openjdk 11 from [here](https://jdk.java.net/11/)

set JAVA_HOME environment variable

```bash
setx JAVA_HOME "C:\Program Files\Java\jdk-11.0.2"
```

Verify java installation

```bash
java -version
```

--------------------------------
Install Python 3.8 or higher
---------------------------------

On Linux

```bash
sudo apt update
sudo apt install python3.8
```

Verify python installation

```bash
python3 --version
```


---------------------------------
Download spark
---------------------------------

Download spark from [here](https://spark.apache.org/downloads.html)

```bash
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvzf spark-3.5.1-bin-hadoop3.tgz
```

set SPARK_HOME environment variable

```bash
echo "export SPARK_HOME=/path/to/spark-3.5.1-bin-hadoop3" >> ~/.bashrc
source ~/.bashrc
```


Optional:
set PYSPARK_PYTHON environment variable to python3
```bash
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc
```


Verify spark installation

```bash
$SPARK_HOME/bin/pyspark
```

---------------------------------


For Windows Hadoop setup:

download hadoop for windows 
https://github.com/steveloughran/winutils


set the HADOOP_HOME environment variable

---------------------------------

Summary:

- Install Java 8 or 11
- Install Python 3.8 or higher
- Download Spark
- Set SPARK_HOME environment variable
- Set PYSPARK_PYTHON environment variable (optional)
- Set HADOOP_HOME environment variable (for windows)

---------------------------------

How/Where to write spark programs for practice / learn?

- spark-shell ( i.e pyspark shell )
- notebooks ( jupyter, zeppelin ) 
- IDE ( pycharm, vs-code )

---------------------------------

How to run python programs in dev?

- using global python interpreter
- using virtual environment ( venv or conda | pipenv )

---------------------------------

create python virtual environment  ( optional)

```bash
python3 -m venv venv
source spark-env/bin/activate
``` 

on windows

```bash
python -m venv venv
spark-env\Scripts\activate
```

---------------------------------