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

#### <i class="icon-file"></i> HOW TO QUICKLY BUILD SCABI

Initial Setup

1. Install Oracle Java 8 Java SE 1.8.0_66
2. Install Git
3. Install Maven
4. Install Eclipse MARS or later
5. Create folder /home/(username)/scabi
6. cd to scabi folder
7. Run command
		git clone https://www.github.com/dilshadmustafa/scabi.git
		git clone https://www.github.com/dilshadmustafa/StorageHandler.git
		git clone https://www.github.com/dilshadmustafa/bigqueue.git
8. Inside the Scabi project, folder named "Download", extract the file  scabi.tar.gz and copy all the jar files from the folder named "dependency-jars". Or alternatively, use any of the Maven pom.xml file inside DilshadDCS_Core or DilshadDCS_CS or DilshadDCS_MS to get all the dependency jar files.
9. In Eclipse, import the following folders DilshadDCS_Core, DilshadDCS_CS, DilshadDCS_MS, Dilshad_StorageHandler, bigqueue and "Test" as **separate** projects using File->Import->Existing Projects into Workspace.
10. In Eclipse, set dependency for DilshadDCS_Core project by right-click->Properties->Java Build Path->Projects->add Dilshad_StorageHandler and bigqueue.
11. In Eclipse, set dependency for DilshadDCS_CS project by right-click->Properties->Java Build Path->Projects->add DilshadDCS_Core, Dilshad_StorageHandler and bigqueue.
12. In Eclipse, set dependency for DilshadDCS_MS project by right-click->Properties->Java Build Path->Projects->add DilshadDCS_Core.
13. In Eclipse, set dependency for Test project by right-click->Properties->Java Build Path->Projects->add DilshadDCS_Core, Dilshad_StorageHandler and bigqueue.
14. In Eclipse, set dependency for bigqueue project by right-click->Properties->Java Build Path->Projects->add Dilshad_StorageHandler.
15. In Eclipse, add jar files for DilshadDCS_Core, DilshadDCS_CS, DilshadDCS_MS and Test project by right-click->Properties->Java Build Path->Libraries->add the jar files obtained in step (8).
16. In Eclipse, add log4j-1.2.17.jar file for bigqueue project by right-click->Properties->Java Build Path->Libraries->add log4j-1.2.17.jar file.
17. Set Java VM arguments by right-click on file DilshadDCS_CS->ComputerServer_D2.java->Run/Debug Settings->Arguments->VM arguments, -Dscabi.local.dir="/home/(username)/testdata/server1_local"
-Dscabi.storage.provider="dfs"
-Dscabi.dfs.mount.dir="/home/(username)/testdata/storage".

18. Set Java VM arguments by right-click on file Test->Test8_Data2_operate_lambda.java (e.g.)->Run/Debug Settings->Arguments->VM arguments, -Dscabi.local.dir="/home/(username)/testdata/driver_local"
-Dscabi.storage.provider="dfs"
-Dscabi.dfs.mount.dir="/home/(username)/testdata/storage".

19. Storage directory (-Dscabi.dfs.mount.dir) or storage system (S3-interface, SeaweedFS, etc.) should be the same for all compute nodes (DilshadDCS_CS->ComputerServer_D2.java) and driver code (e.g. Test->Test8_Data2_operate_lambda.java) as the storage system forms the Data Ring.

20. Run MongoDB or Cassandra server.

21. In Eclipse, right-click on files, DilshadDCS_MS->MetaServer.java, DilshadDCS_CS->ComputerServer_D2.java (compute service), Test->Test->Test8_Data2_operate_lambda.java (driver code example) and click Run As->Java Aplication.

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
