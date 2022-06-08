##Project Setup Guide
###Setting up PySpark and JDK

Running spark on Windows are involves a lot of setup, so install everything carefully and as shown.
Administration rights will be needed. Make sure that you already have installation of [Python 3.9++](https://www.python.org/downloads/)
and [PyCharm](https://www.jetbrains.com/pycharm/download) IDE.

1. Make sure you have the Java 8 JDK installed. If you don't have it installed, 
download and install [JDK](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).

    **DO NOT INSTALL LATEST JAVA, SPARK IS ONLY COMPATIBLE WITH JAVA 8 OR 11.**
2. Download and install **latest** stable [Spark](https://spark.apache.org/downloads.html) (pre-built)
3. Spark is packaged into *.tgz file, you might need to install [winRAR](https://www.rarlab.com/) to open *.tgz files
4. Extract the Spark archive, and copy its contents into **C:\spark** after creating that directory. You should end up with directories like **c:\spark\bin**, **c:\spark\conf**, etc.
5. Download [winutils.exe](https://sundog–s3.amazonaws.com/winutils.exe) and move it into a **C:\winutils\bin** folder that you’ve to create.
6. Create a **c:\tmp\hive** directory, using CMD run commands:
   1. Change directory
      ````shell 
       cd c:\winutils\bin
      ````
   2. Grant permissions
      ````shell
      winutils.exe chmod 777 c:\tmp\hive
      ````
7. Open the **c:\spark\conf** folder, and make sure “File Name Extensions” is checked in the “view” tab of Windows Explorer. 
Rename the log4j.properties.template file to log4j.properties. Edit this file (using Wordpad or something similar) and change 
the error level from INFO to ERROR for log4j.rootCategory
8. Right-click your Windows menu, select System. 
Click on “Advanced System Settings” and then the “Environment Variables” button.
9. Add the following new **USER** variables:
   1. SPARK_HOME c:\spark
   2. JAVA_HOME (the path you installed the JDK to in step 1, for example C:\JDK)
   3. HADOOP_HOME c:\winutils
   4. PYTHONPATH c:\spark\python
10. Add the following paths to your PATH user variable:
    1. %SPARK_HOME%\bin
    2. %JAVA_HOME%\bin
11. Close the environment variable screen and the control panels.
12. Test the pySpark:
    1. Open CMD
    2. Enter: **cd c:\spark**
    3. Enter: **pyspark**
    4. Enter: **rdd = sc.textFile(“README.md”)**
    5. Enter: **rdd.count()**
    6. You should get a count of the number of lines in that file!
    7. Enter: quit()
    8. If no errors seen than you've got everything correct
    
###Setting up Project
To be able to run the spark job, please install libraries that you can locate in requirements.txt file.
In terminal, execute next command to install them:
````shell
pip install -r requirements.txt
````

###Project Overview
The main job is located in pyspark.py file. The script there uses files from RAW directory 
(each csv file represents all ads for Riga region on the particular date) to extract, transform and analyse the flats commercials that available in Riga.
The task is to implement 10 missing functions (you can find them by searching for **pass** keyword). Those are:
* set_region_and_street
* set_top_floor
* set_categories
* clean_price
* commercials_by_house_type
* commercials_by_category
* top_zones_by_commercial_count
* average_price_in_regions_for_category
* count_floors_for_category
* top_floors
The documentation can be found inside the function's body.

The result can be checked by running all or particular test. You can do it from IDE or from terminal.
Tests located in **.\test\pyTest.py**
Run test from IDE: 
* Push green triangle near the test (particular test)
* Right mouse button on file **.\test\pyTest.py** -> push green triangle (run all tests)
Run tests from terminal: 
````shell
pytest .\test\pyTest.py  # all test
pytest .\test\pyTest.py::test_top_floor_extract  # particular test  
````

After the successful implementation of all functions, you should be able to run the main script located in
**py_spark.py** (hit the green triangle near the ``if __name__ == '__main__':`` statement)