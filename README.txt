Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.

Scabi is a simple, light-weight Distributed Computing & Storage micro framework with a Cluster system written purely in Java. 

Scabi provides high performance computing and storage with ease of use. Users can get started on using Scabi within a few minutes.

Scabi is free of cost to use. Please refer the License.txt file for complete details on the license and terms and conditions.

Please refer the Scabiv0.2.pptx to get started on using Scabi.

HOW TO QUICKLY RUN SCABI:-

(1) Install Oracle Java 8 Java SE 1.8.0_66
(2) Install MongoDB v3.2.1 with default settings, without enabling Login password and security certificate
(3) Create data folder for MongoDB, /home/<username>/data/db
(4) Start MongoDB server, sudo mongod --dbpath /home/<username>/data/db
(5) Download scabiv0.2.tar.gz from Download folder in Scabi’s GitHub project
(6) Unzip scabiv0.2.tar.gz to a folder /home/<username>/scabi
(7) cd /home/<username>/scabi
(8) Start Meta Server, 
	./start_meta.sh &
(9) Start Compute Servers,
	./start_compute.sh 5001 localhost 5000 1000 &
	./start_compute.sh 5002 localhost 5000 1000 &

      To start Compute Servers in other machines, enter command as below,
	./start_compute.sh <ComputeServer_Port> <MetaServer_HostName> <MetaServer_Port> [<NoOfThreads> [debug]] &
	
(10) Run example code inside the examples folder in /home/<username>/scabi,

cd examples
java -cp "../dependency-jars/*":"../*":. Example1
java -cp "../dependency-jars/*":"../*":. Example1_2
java -cp "../dependency-jars/*":"../*":. Example1_3
java -cp "../dependency-jars/*":"../*":. Example1_4
java -cp "../dependency-jars/*":"../*":. Example2
java -cp "../dependency-jars/*":"../*":. Example3
java -cp "../dependency-jars/*":"../*":. Example4
java -cp "../dependency-jars/*":"../*":. Example5

(11) To run Meta Server and Compute Server from Windows, use the .bat files,
	start_meta.bat
	start_compute.bat 5001 localhost 5000 1000

HOW TO QUICKLY BUILD SCABI:-

Initial Setup

(1) Install Oracle Java 8 Java SE 1.8.0_66
(2) Install Git
(3) Install Maven
(4) Create folder /home/<username>/scabi
(5) cd to scabi folder
(6) Run command
	git clone https://www.github.com/dilshadmustafa/scabi.git

Build Scabi Core scabi_core.jar
1. cd to DilshadDCS_Core folder in /home/<username>/scabi
2. Run command
	mvn package
3. The file scabi_core.jar will be created

Build Scabi Meta Server scabi_meta.jar

1. Copy scabi_core.jar file created in above step to folder DilshadDCS_MS
2. cd to DilshadDCS_MS folder in /home/<username>/scabi
3. Include scabi_core.jar in Maven java classpath before compiling with Maven
4. Run command
	mvn package
5. The file scabi_meta.jar will be created
6. cd to target folder
7. Include scabi_core.jar in java classpath before running Meta Server
8. Run below command to run Meta Server with default settings (MongoDB should be installed already)
	java -jar scabi_meta.jar

Build Scabi Compute Server scabi_compute.jar

1. Copy scabi_core.jar file created in above step to folder DilshadDCS_CS
2. cd to DilshadDCS_CS folder in /home/<username>/scabi
3. Include scabi_core.jar in Maven java classpath before compiling with Maven
4. Run command
	mvn package
5. The file scabi-compute.jar will be created
6. cd to target folder
7. Include scabi_core.jar in java classpath before running Compute Server
8. Run below command to run Compute Server, (Meta Server should be started already)
	java -jar scabi_compute.jar 5001 localhost 5000 1000

To compile examples,

1. Include scabi_core.jar in java classpath before compiling
2. cd examples
2. javac -cp "../dependency-jars/*":"../*":. Example1
   from local download folder
3. Or javac -cp "DilshadDCS_Examples/target/dependency-jars/*":. Example1
   from local git clone folder


