##Project setup guide
###Setting up pySpark and JDK

Running spark on Windows are involves a lot of setup, so install everything carefully and as shown.
Administration rights will be needed.

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






Download and install [PyCharm](https://www.jetbrains.com/pycharm/download) IDE.
    

install libs :
````shell
pip install -r requirements.txt
````


run tests from terminal: 
````shell
pytest .\test\pyTest.py
pytest .\test\pyTest.py::{test_name}
````