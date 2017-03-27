Welcome to Scabi
===================

![Scabi Logo](https://raw.githubusercontent.com/dilshadmustafa/scabi/master/Scabi_logo.jpg)

<br>
<br>

[![](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=H4V87SN5M2GG2)

Introduction
-------------

**Scabi** (<b>Sca</b>le-out <b>bi</b>g-data) is a simple, light-weight cluster computing and storage framework for Big Data processing, Ensemble Machine Learning and Map/Reduce in pure Java. With Scabi's Data-driven cluster, create Ensemble Machine Learning models for super massive datasets, implement iterative and complex Map/Reduce and parallel algorithms and configure Scabi's Data Ring to use any storage: Scality, RedHat CephFS, OrangeFS, RedHat Gluster, SeaweedFS, Minio, IBM Cleversafe, etc. 

> **Objectives:**

> - Process Petabytes to Exabytes+ of data using billions of massively parallel Scabi Data Units (DU).
> - Create Ensemble Machine Learning models for super massive datasets with algorithm and ML library of your choice.
> - Use Data-driven and Compute-driven framework to implement iterative and complex Map/Reduce and parallel algorithms at Web Scale.
> - Configure Scabi's Data Ring to use storage of your choice: Scality, RedHat CephFS, OrangeFS, RedHat Gluster, SeaweedFS, Minio, IBM Cleversafe, etc.

## Cluster Computing Architectures ##

> **CONVERGED SYSTEM**

 >- Converged System combines computing cluster with storage system. 
> - Nodes are built with computing power and storage.
> - Converged System can provide the advantage of Data Locality in best case scenario when computing is done on the same node containing the data to be processed.
> - The disadvantage is when we start storing previous months PetaBytes+ scale datasets, the storage requirements will soon become out of proportion (often exponentially) compared to computing    requirements. This will require addition of new nodes.

-------------

> **DIVERGED SYSTEM**

> - Diverged System separates computing cluster from storage system.
> - Compute nodes are built using arrays of GPU-like cores.
> - Storage nodes are built with embedded light-weight processor (e.g.    Raspberry Pi, Arduino, etc. to name a few) with processing speed sufficient enough to handle the speed of the underlying storage technology used to read and write data.
> - As the computing cluster and storage system is separate, storing previous months PetaBytes+ scale datasets will only require increase in capacity of storage system.

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

#### <i class="icon-file"></i> HOW TO QUICKLY BUILD SCABI USING MAVEN

Initial Setup

1. Install Oracle Java 8 Java SE 1.8.0_66
2. Install Git
3. Install Maven 3.0 or later
4. Create folder /home/(username)/myworkspace 
Replace **(username)** with your user name in all the steps.
5. cd to /home/(username)/myworkspace folder
6. In terminal window, from /home/(username)/myworkspace folder, run commands,

		git clone https://www.github.com/dilshadmustafa/scabi.git
		git clone https://www.github.com/dilshadmustafa/StorageHandler.git
		git clone https://www.github.com/dilshadmustafa/bigqueue.git

		After the git clone, three folders should be created: 
		/home/(username)/myworkspace/scabi, 
		/home/(username)/myworkspace/StorageHandler, 
		/home/(username)/myworkspace/bigqueue
		
7. cd to /home/(username)/myworkspace/scabi folder.
8. In terminal window, from /home/(username)/myworkspace/scabi folder, run command,

		mvn package
		
		(In case of connection timeout to Maven repository or no response, Ctrl-C in the terminal and try again few times)
		
		File scabi-0.2.3.jar will be created inside /home/(username)/myworkspace/scabi/target folder
		
		All the dependency jar files will be copied to /home/(username)/myworkspace/scabi/target/dependency-jars folder
9. In a separate terminal, start MongoDB or Cassandra server,

		sudo mongod --dbpath /home/(username)/data/db
10. In a separate terminal, to run Meta Service, run commands,

		cd /home/(username)/myworkspace/scabi/target
		java -cp scabi-0.2.3.jar:"dependency-jars/*":. com.dilmus.dilshad.scabi.ms.MetaServer

11. In another terminal, to run Compute Service, run commands,

		Create folders /home/(username)/testdata/server1_local, /home/(username)/testdata/storage

		Replace (username) with your user name.
		
		cd /home/(username)/myworkspace/scabi/target
		java -cp scabi-0.2.3.jar:"dependency-jars/*":. -Dscabi.local.dir="/home/(username)/testdata/server1_local" -Dscabi.storage.provider="dfs" -Dscabi.dfs.mount.dir="/home/(username)/testdata/storage" com.dilmus.dilshad.scabi.cs.D2.ComputeServer_D2 5001 localhost 5000

		Or to enable debugging details, run commands,
		
		cd /home/(username)/myworkspace/scabi/target
		java -cp scabi-0.2.3.jar:"dependency-jars/*":. -Dscabi.local.dir="/home/(username)/testdata/server1_local" -Dscabi.storage.provider="dfs" -Dscabi.dfs.mount.dir="/home/(username)/testdata/storage" com.dilmus.dilshad.scabi.cs.D2.ComputeServer_D2 5001 localhost 5000 5 debug 
		
		To use SeaweedFS as Storage provider, run commands,

		cd /home/(username)/myworkspace/scabi/target
		java -cp scabi-0.2.3.jar:"dependency-jars/*":. -Dscabi.local.dir="/home/(username)/testdata/server1_local" -Dscabi.storage.provider="seaweedfs" -Dscabi.seaweedfs.config="localhost-8888" com.dilmus.dilshad.scabi.cs.D2.ComputeServer_D2 5001 localhost 5000

		Or to enable debugging details, run commands,
		
		cd /home/(username)/myworkspace/scabi/target
		java -cp scabi-0.2.3.jar:"dependency-jars/*":. -Dscabi.local.dir="/home/(username)/testdata/server1_local" -Dscabi.storage.provider="seaweedfs" -Dscabi.seaweedfs.config="localhost-8888" com.dilmus.dilshad.scabi.cs.D2.ComputeServer_D2 5001 localhost 5000 5 debug 
12. In another terminal, to run example Driver code for Data-driven framework, run commands,

		Create folders /home/(username)/testdata/driver_local, /home/(username)/testdata/storage

		Replace (username) with your user name.
		
		cd /home/(username)/myworkspace/scabi/target
		java -cp scabi-0.2.3.jar:"dependency-jars/*":. -Dscabi.local.dir="/home/(username)/testdata/driver_local" -Dscabi.storage.provider="dfs" -Dscabi.dfs.mount.dir="/home/(username)/testdata/storage" Test8_Data2_operate_lambda
		
		To use SeaweedFS as Storage provider, run commands,
		
		cd /home/(username)/myworkspace/scabi/target
		java -cp scabi-0.2.3.jar:"dependency-jars/*":. -Dscabi.local.dir="/home/(username)/testdata/driver_local" -Dscabi.storage.provider="seaweedfs" -Dscabi.seaweedfs.config="localhost-8888" Test8_Data2_operate_lambda

13. Storage system configuration as in the above run commands (S3-interface Storage system, SeaweedFS, etc.) for example -Dscabi.seaweedfs.config="localhost-8888" or Storage directory for example -Dscabi.dfs.mount.dir="/home/(username)/testdata/storage" should point to the same Storage system for all Compute Service nodes (scabi folder->DilshadDCS_CS->ComputerServer_D2.java) and Driver code (e.g. scabi folder->Test->Test8_Data2_operate_lambda.java) as the same Storage system only forms the Data Ring.

	IStorageHandler.java interface provides a single view of this Data Ring formed by the Storage system but this interface's actual implementation class may actually use multiple Storage systems of same or different type. For example multiple S3-interface Storage systems or a mix of multiple different types of Storage systems each accessed through different ways (S3, HTTP, REST, APIs) may be used by the actual implementation class of IStorageHandler.java interface.

#### <i class="icon-file"></i> HOW TO QUICKLY BUILD SCABI USING ECLIPSE

Initial Setup

1. Install Oracle Java 8 Java SE 1.8.0_66
2. Install Git
3. Install Maven 3.0 or later
4. Install Eclipse Mars or later
5. Create folder /home/(username)/myworkspace
Replace **(username)** with your user name in all the steps.
6. cd to /home/(username)/myworkspace folder
7. In terminal window, from /home/(username)/myworkspace folder, run commands,

		git clone https://www.github.com/dilshadmustafa/scabi.git
		git clone https://www.github.com/dilshadmustafa/StorageHandler.git
		git clone https://www.github.com/dilshadmustafa/bigqueue.git

		After the git clone, three folders should be created: 
		/home/(username)/myworkspace/scabi, 
		/home/(username)/myworkspace/StorageHandler, 
		/home/(username)/myworkspace/bigqueue
8. Inside /home/(username)/myworkspace/scabi/Download folder, extract the file scabi.tar.gz and copy all the jar files from the folder named "dependency-jars". 

	Or alternatively, use the pom.xml file in /home/(username)/myworkspace/scabi/pom.xml and Maven to get all the dependency jar files as below:
	
		cd /home/(username)/myworkspace/scabi
		mvn package
		
		(In case of connection timeout to Maven repository or no response, Ctrl-C in the terminal and try again few times)
				
		All the dependency jar files will be copied to /home/(username)/myworkspace/scabi/target/dependency-jars folder
9. In Eclipse, import the following folders 

		/home/(username)/myworkspace/scabi/DilshadDCS_Core, 
		/home/(username)/myworkspace/scabi/DilshadDCS_CS,
		/home/(username)/myworkspace/scabi/DilshadDCS_MS, 
		/home/(username)/myworkspace/StorageHandler/Dilshad_StorageHandler 
		/home/(username)/myworkspace/scabi/Test 
	as **separate** projects using File->Import->Existing Projects into Workspace.

10. In Eclipse, create new Java project and point to existing Java source code in /home/(username)/myworkspace/bigqueue/src/main/java folder.
11. In Eclipse, set dependency for DilshadDCS_Core project by right-click->Properties->Java Build Path->Projects->add Dilshad_StorageHandler and bigqueue.
12. In Eclipse, set dependency for DilshadDCS_CS project by right-click->Properties->Java Build Path->Projects->add DilshadDCS_Core, Dilshad_StorageHandler and bigqueue.
13. In Eclipse, set dependency for DilshadDCS_MS project by right-click->Properties->Java Build Path->Projects->add DilshadDCS_Core.
14. In Eclipse, set dependency for Test project by right-click->Properties->Java Build Path->Projects->add DilshadDCS_Core, Dilshad_StorageHandler and bigqueue.
15. In Eclipse, set dependency for bigqueue project by right-click->Properties->Java Build Path->Projects->add Dilshad_StorageHandler.
16. In Eclipse, add jar files for DilshadDCS_Core, DilshadDCS_CS, DilshadDCS_MS and Test project by right-click->Properties->Java Build Path->Libraries->add the jar files obtained in step (8).
17. In Eclipse, add log4j-1.2.17.jar file for bigqueue project by right-click->Properties->Java Build Path->Libraries->add log4j-1.2.17.jar file.
18. In Eclipse, set Java VM arguments by right-click on file DilshadDCS_CS->ComputerServer_D2.java->Run/Debug Settings->Arguments->VM arguments,
		
		-Dscabi.local.dir="/home/(username)/testdata/server1_local"
		-Dscabi.storage.provider="dfs"
		-Dscabi.dfs.mount.dir="/home/(username)/testdata/storage"

		Create folders /home/(username)/testdata/server1_local, /home/(username)/testdata/storage

		Replace (username) with your user name.
		
		or to use SeaweedFS as Storage provider: 
		
		-Dscabi.local.dir="/home/(username)/testdata/server1_local"
		-Dscabi.storage.provider="seaweedfs"
		-Dscabi.seaweedfs.config="localhost-8888".
	
19. In Eclipse, set Java VM arguments by right-click on file Test->Test8_Data2_operate_lambda.java (e.g.)->Run/Debug Settings->Arguments->VM arguments, 
	
		-Dscabi.local.dir="/home/(username)/testdata/driver_local"
		-Dscabi.storage.provider="dfs"
		-Dscabi.dfs.mount.dir="/home/(username)/testdata/storage" 

		Create folders /home/(username)/testdata/driver_local, /home/(username)/testdata/storage

		Replace (username) with your user name.
		
		or to use SeaweedFS as Storage provider: 
		
		-Dscabi.local.dir="/home/(username)/testdata/driver_local"
		-Dscabi.storage.provider="seaweedfs"
		-Dscabi.seaweedfs.config="localhost-8888".

20. Storage system configuration as in the above run commands (S3-interface Storage system, SeaweedFS, etc.) for example -Dscabi.seaweedfs.config="localhost-8888" or Storage directory for example -Dscabi.dfs.mount.dir="/home/(username)/testdata/storage" should point to the same Storage system for all Compute Service nodes (scabi folder->DilshadDCS_CS->ComputerServer_D2.java) and Driver code (e.g. scabi folder->Test->Test8_Data2_operate_lambda.java) as the same Storage system only forms the Data Ring.

	IStorageHandler.java interface provides a single view of this Data Ring formed by the Storage system but this interface's actual implementation class may actually use multiple Storage systems of same or different type. For example multiple S3-interface Storage systems or a mix of multiple different types of Storage systems each accessed through different ways (S3, HTTP, REST, APIs) may be used by the actual implementation class of IStorageHandler.java interface.

21. In a separate terminal, start MongoDB or Cassandra server,

		sudo mongod --dbpath /home/(username)/data/db

22. In Eclipse, right-click on files, DilshadDCS_MS->MetaServer.java, DilshadDCS_CS->ComputerServer_D2.java (Compute Service), Test->Test8_Data2_operate_lambda.java (Driver code example) and click Run As->Java Application.

**Alternate method 1 - Import Maven project into Eclipse**

1. Do steps (1) to (7) as given above in "HOW TO QUICKLY BUILD SCABI USING ECLIPSE".
2. In Eclipse, File->Import->Import from Existing Maven project and choose file /home/(username)/myworkspace/scabi/pom.xml. You need to have Eclipse Maven plugin installed.
3. Do steps (18) to (22).

**Alternate method 2 - Generate Eclipse project using Maven**

1. Do steps (1) to (7) as given above in "HOW TO QUICKLY BUILD SCABI USING ECLIPSE".
2. cd to /home/(username)/myworkspace/scabi folder.
3. In terminal window, from /home/(username)/myworkspace/scabi folder, run command,
		
		mvn -npr eclipse:eclipse

		You may edit /home/(username)/myworkspace/scabi/pom.xml to explicitly specify maven eclipse plugin version if really needed as below:
		<plugin>
		   <groupId>org.apache.maven.plugins</groupId>
		   <artifactId>maven-eclipse-plugin</artifactId>
		   <version>put eclipse plugin version here</version>
		   <configuration>
			...
4. In Eclipse, import the folder /home/(username)/myworkspace/scabi using File->Import->Existing Projects into Workspace.		
5. Do steps (18) to (22).

**RUNNING SEAWEEDFS - QUICK START**

1. Download SeaweedFS from https://github.com/chrislusf/seaweedfs/releases and extract to /home/(username)/seaweed folder.

2. To start SeaweedFS Master, Volume and Filer servers, run commands,

		cd /home/(username)/seaweed
	
		Create folders /home/(username)/mystorage/mystorage1, /home/(username)/mystorage/forfiler

		Replace (username) with your user name.
		
		./weed master -volumeSizeLimitMB=350
	
		./weed volume -idleTimeout=1000000 -max=10000 -mserver="localhost:9333" -dir="/home/(username)/mystorage/mystorage1"
	
		./weed filer -dir="/home/(username)/mystorage/forfiler"
3. Open URL http://localhost:8888/  to view the files in SeaweedFS Filer.

Copyright
-------------------

Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.

License
-------------

Please refer LICENSE.txt file for complete details on the license and terms and conditions.

About The Author
--------------------

Dilshad Mustafa is the creator and programmer of Scabi framework and Cluster. He is also Author of Book titled “Tech Job 9 to 9”. He is a Senior Software Architect with 16+ years experience in Information Technology industry. He has experience across various domains, Banking, Retail, Materials & Supply Chain.

He completed his B.E. in Computer Science & Engineering from Annamalai University, India and completed his M.Sc. in Communication & Network Systems from Nanyang Technological University, Singapore.
