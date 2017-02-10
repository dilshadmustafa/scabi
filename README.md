Welcome to Scabi
===================

![Scabi Logo](https://raw.githubusercontent.com/dilshadmustafa/scabi/master/Scabi_logo.jpg)

Introduction
-------------

**Scabi** (**Scale**-out **big**-data) is a simple, light-weight cluster computing and storage framework for Big Data processing, Ensemble Machine Learning and Map/Reduce in pure Java. With Scabi's Data-driven cluster, create Ensemble Machine Learning models for super massive datasets, implement iterative and complex Map/Reduce and parallel algorithms and configure Scabi's Data Ring to use any storage: Scality, RedHat CephFS, OrangeFS, RedHat Gluster, SeaweedFS, Minio, IBM Cleversafe, etc. 

> **Objectives:**

> - Process Petabytes to Exabytes+ of data using billions of massively parallel Scabi Data Units (DU).
> - Create Ensemble Machine Learning models for super massive datasets with algorithm and ML library of your choice.
> - Use Data-driven and Compute-driven framework to implement iterative and complex Map/Reduce and parallel algorithms at Web Scale.
> - Configure Scabi's Data Ring to use storage of your choice: Scality, RedHat CephFS, OrangeFS, RedHat Gluster, SeaweedFS, Minio, IBM Cleversafe, etc.

## Architectural Design ##

![Scabi Architectural Design](https://raw.githubusercontent.com/dilshadmustafa/scabi/v0.2.3/Documentation/Scabi_1.jpg)

![Architectural Design_2](https://raw.githubusercontent.com/dilshadmustafa/scabi/v0.2.3/Documentation/Scabi_2.jpg)

![Architectural Design_3](https://raw.githubusercontent.com/dilshadmustafa/scabi/v0.2.3/Documentation/Scabi_3.jpg)

## Documentation ##

Please refer Scabi.pptx in Documentation folder to get started on using Scabi.

#### <i class="icon-file"></i> HOW TO QUICKLY RUN SCABI

1. Install Oracle Java 8 Java SE 1.8.0_66
2. Install MongoDB v3.2.1 with default settings, without enabling Login password and security certificate
3. Create data folder for MongoDB, /home/(username)/data/db
4. Start MongoDB server, sudo mongod --dbpath /home/(username)/data/db
5. Download scabi.tar.gz from Download folder in Scabi's GitHub project
6. Unzip scabi.tar.gz to a folder /home/(username)/scabi
7. cd /home/(username)/scabi

8. Start Meta Server, 
	./start_meta.sh

9. Start Compute Servers,
	./start_compute.sh 5001 localhost 5000 1000
	./start_compute.sh 5002 localhost 5000 1000

   	To start Compute Servers in other machines and ports, enter command as below,
	./start_compute.sh (ComputeServer_Port) (MetaServer_HostName) (MetaServer_Port) [(NoOfThreads) [debug]]
	
	To run Meta Server and Compute Server from Windows, use the .bat files,
	start_meta.bat
	start_compute.bat 5001 localhost 5000 1000

10. Run example code inside the examples folder in /home/[username]/scabi,

		cd examples
		java -cp "../dependency-jars/*":"../*":. Example1
		java -cp "../dependency-jars/*":"../*":. Example1_2
		java -cp "../dependency-jars/*":"../*":. Example1_3
		java -cp "../dependency-jars/*":"../*":. Example1_4
		java -cp "../dependency-jars/*":"../*":. Example2
		java -cp "../dependency-jars/*":"../*":. Example3
		java -cp "../dependency-jars/*":"../*":. Example4
		java -cp "../dependency-jars/*":"../*":. Example5

11. Commandline arguments

**./start_meta.sh**

Usage : 
./start_meta.sh (No arguments) to use default settings, local host, port 5000, connect to local MongoDB server and port

./start_meta.sh (MetaServer_Port) [debug]

./start_meta.sh (MetaServer_Port) (Database_HostName) (Database_Port) [debug]

**./start_compute.sh**

Usage : 
./start_compute.sh (ComputeServer_Port) (MetaServer_HostName) (MetaServer_Port) [(NoOfThreads) [debug]]

#### <i class="icon-file"></i> HOW TO QUICKLY BUILD SCABI

Initial Setup

1. Install Oracle Java 8 Java SE 1.8.0_66
2. Install Git
3. Install Maven
4. Create folder /home/(username)/scabi
5. cd to scabi folder
6. Run command
	git clone https://www.github.com/dilshadmustafa/scabi.git

Build Scabi Core scabi_core.jar

1. cd to DilshadDCS_Core folder in /home/(username)/scabi
2. Run command
	mvn package
3. The file scabi_core.jar will be created

Build Scabi Meta Server scabi_meta.jar

1. Copy scabi_core.jar file created in above step to folder DilshadDCS_MS
2. cd to DilshadDCS_MS folder in /home/(username)/scabi
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
2. cd to DilshadDCS_CS folder in /home/(username)/scabi
3. Include scabi_core.jar in Maven java classpath before compiling with Maven
4. Run command
	mvn package
5. The file scabi-compute.jar will be created
6. cd to target folder
7. Include scabi_core.jar in java classpath before running Compute Server
8. Run below command to run Compute Server, (Meta Server should be started already)
	java -jar scabi_compute.jar 5001 localhost 5000 1000

To compile examples,

To compile from local download folder:-

	1. cd /home/(username)/scabi or your local download extract folder
	2. cd examples
	3. Include scabi_core.jar in java classpath before compiling
	4. javac -cp "../dependency-jars/*":"../*":. Example1

Or to compile from local git clone folder, follow steps as below:-

	1. cd /home/(username)/scabi/ or your local git clone folder
	2. cd DilshadDCS_Examples
	3. mvn package, scabi_core.jar file and dependency-jars folder will be created
	4. cd target/classes
	5. javac -cp "../dependency-jars/*":"../*":. Example1

Copyright
-------------------

Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.

License
-------------

Please refer License.txt file for complete details on the license and terms and conditions.

About The Author
--------------------

Dilshad Mustafa is the creator and programmer of Scabi framework and Cluster. He is also Author of Book titled “Tech Job 9 to 9”. He is a Senior Software Architect with 16+ years experience in Information Technology industry. He has experience across various domains, Banking, Retail, Materials & Supply Chain.

He completed his B.E. in Computer Science & Engineering from Annamalai University, India and completed his M.Sc. in Communication & Network Systems from Nanyang Technological University, Singapore.
