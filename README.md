# Data Stream Processing and Analytics

## Requirements:
The program should be run on Linux operating system.
Before running the project install the following software:

*  The project requires MySQL to be installed on the machine running it.

*  The project requires Kafka to be installed on the machine running it.

## Installation and Initial Configuration:
We will not post instructions for Kafka installation as it was part of this course.
Notice that it is required to use ***kafka_2.12-2.2.0.tgz*** when installing.

However, we will provide instuctions for installing and setting up the ***MySQL.***
The instructions originated from:

https://phoenixnap.com/kb/how-to-install-mysql-on-ubuntu-18-04?fbclid=IwAR1MbQUM_PRQr9SxyQbtm9_9RUA7xbtDEjO84b-wD6y-Uoy6qhNXz_zq18o

### Install MySQL
Run the following commands:
```
wget –c https://dev.mysql.com/get/mysql-apt-config_0.8.11-1_all.deb
sudo dpkg -i mysql-apt-config_0.8.11-1_all.deb
```

This opens old school installation window. Leave the default settings and click OK.

Proceed with the following commands:
```
sudo apt-get update
sudo apt-get install mysql-server
```

During the mysql-server installation you may be asked to create root user password.
This is the password you need to remember for future steps.
If you are finding difficult to pick a password, here is a good one: ***Amazing_123***

Proceed with the following commands:
```
sudo mysql_secure_installation
```

During this installation you will be asked for the following:
*  For the first option (VALIDATE PASSWORD PLUGIN) press N
*  Secondly, set the root user password. If it was created in the previous steps,
you will have to enter old one first and then proceed setting the new one.
Make sure to remember this one as you will need it again.
*  For the next 4 questions press Y

### Initial Configuration

After the MySQL is installed, database and user are needed to be created. 
This can be done by runnnig the setup.sh bash script.
Navigate to ***data-stream-processing-and-analytics/*** and use command:
```
sudo ./setup.sh
```
Here you will be asked to provide the root user password previously created.

After running this command, the default database is created and all of its table content is loaded.
You can specify the preferred dataset by providing its size as a parameter (1k or 10k). 
The currently available databases can be seen in Database (static_database_1k.sql and static_database_10k.sql).

The following command will set up the default database with the 1k user dataset:
```
sudo ./setup.sh 1k
```
Creating tables in the database, populating them and filling Kafka will be 
explained more in details in the next section.

To verify this whole process went correctly run the following commands:
```
sudo mysql -u root -p
use static_database
show tables;
```

You will be asked for your root password again.
If everything is okay it will execute commands without errors and you will see 12 tables.

## Running the project:

Before each run of the project, make sure Kafka is also started. 
You can do it by navigating to Kafka folder and using these commands:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### Dataset
Because data originates from the CSV files that are too large, 
they are not included in our gitlab repository.
This means, that you should add them somewhere manually. 
Ideally, the location we have intended for that is:

data-stream-processing-and-analytics/Data.

Go ahead and create Data directory there and just copy paste directories  
***10k-users-sorted*** and ***1k-users-sorted*** obtained from zip files.
The default dataset which will be used is ***10k** one and this is described in file:

data-stream-processing-and-analytics/dspa-project/config.xml

If you wish to change the location from which data is read you can do this by:
1.  Modifying the mentioned config.xml file
2.  Adding ***-config PATH*** parameter when running the program where ***PATH***
is leading to a different config.xml file that has the same structure as ours.

### First run of the project

#### Creating and filling the database
The setup.sh creates a database called static_database and creates a user svilen that is used to operate with the databse.
It also creates and fills the tables in database with the specified database setup (the default database is set to static_database_10k.sql).

This is how the code currently works: ***ONLY*** if the tables do not already exist, 
and they should exist on the first run of the project because of the setup.sh script,
they will be populated from the CSV files that are located on the paths in the config file.
We use 12 CSV files from the tables folder in dataset. This CSV files first fill
the tables and then database is used to create similarity matrix between users for task 2.

If you ever wish to change this data, modifying the ***config*** file or specifying 
path to a new one is not enough. Program should be run with ***-delete*** parameter.

***CAUTION:*** Process of creating, filling tables and creating similarity matrix
for task 2 can take up to 1 hour. This is why we fill it using the script during
Initial Configuration within the setup.sh script. It can be skipped at any point, first run or 
Nth run of the project by running script which imports it all and is located at 
***data-stream-processing-and-analytics/Database/:***

If you wish to use 1k dataset
```
./import_database.sh 1k
```
If you wish to use 10k dataset
```
./import_database.sh 10k
```

#### Filling Kafka
Unlike database which gets populated in the setup.sh, this is not the case with Kafka.
If you want to fill Kafka you must explicitly provide the following parameters:
```
-loadKafkaComments maximumNumberOfComments
-loadKafkaPosts maximumNumberOfPosts
-loadKafkaLikes maximumNumberOfLikes
```

In case that ***maximumNumber*** values are not specified, then the whole file
at the location in the config file will be loaded. However if you do specify 
them please pay attention not to exceed:
* 632 043 for maximumNumberOfComments in 1k dataset
* 20 096 289 for maximumNumberOfComments in 10k dataset
* 173 402 for maximumNumberOfPosts in 1k dataset
* 5 520 843 for maximumNumberOfPosts in 10k dataset
* 662 891 for maximumNumberOfLikes in 1k dataset
* 21 148 772 for maximumNumberOfLikes in 10k dataset

If you ever wish to change this data, modifying the ***config*** file or specifying 
path to a new one is not enough. First you must delete all the topics with:
* bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic comment-topic
* bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic like-topic
* bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic post-topic

After that, you need to run the program again with ***-loadKafka*** parameters

***Once you fill both database and Kafka you do not need to redo it for other project runs!***


### Tasks and Outputs

We have labeled all the tasks in the following way:

| Name | Description | Parameter Code |
| ------ | ------ | ------ |
| Task1.1 | Comments count | 1.1 |
| Task1.2 | Replies count | 1.2 |
| Task1.3 | People count | 1.3 |
| Task2_Static | Static Recommendations| 2.S |
| Task2_Dynamic | Dynamic Recommendations | 2.D |
| Task3 | Unusual activity | 3 |

When running the program, depending on which task you wish to run, you should 
add the parameter ***-task*** PARAMETER_CODE 1 PARAMETER_CODE 2 ... PARAMETER_CODE N.
Basically, it is the ***-task*** and then all codes (from the table) separated with space 

For all tasks and its subtasks you will find the outputs at
***data-stream-processing-and-analytics/Output/*** 
and then corresponding Task number and file/folder that matches the description.
Also for every run of the program, date and time when it is ran is added to the
naming of the file so they can be distinguished from multiple runs.

In case of Task1 our output is in the form of 3 files each with 3 columns:

Date (end of time window), PostId, Corresponding Count

In case of Task2 our output is in the form of 10 files (one per user) with 5 colums:

Suggestion 1, Suggestion 2, Suggestion 3, Suggestion 4, Suggestion 5

Additional explanation of Task2: We output this per time window where these
5 columns are the top 5 user recommendations among all active users. It can occur
that current time window has less than 5 active users in which case cells will 
have None value. Otherwise, value of cell is a tuple of (PersonId, Similarity_Score).

In case of Task3 our output is in the form of 1 file with 5 colums:

EventType (comment or post), EventId, PersonId, CreationDate, PlaceId (where event occured)

### Running Tests
Basically, whenever changing dataset or configuration, Kafka and/or Database
should be refilled in the way it was already previously described. In case of 
running tests, this is not different at all and should be done as well.

After that, when running the program, you can specify the config file from which
the data for that task will be loaded. Those config files can be located at:

data-stream-processing-and-analytics/Tests/Task_NUMBER

Below you can see an example for setting up the parameters for a test for task 1.1:

```
-loadKafkaLikes -loadKafkaComments -loadKafkaPosts -config ../Tests/Task1/Task1_1/config.xml -task 1.1
```

Also, the expected output is under the same folder in the Output folder.

To check if the produced output matches the expected output one can use the provided check_test_output.py script in the Tests folder. Example use of the script is as follows:
```
./check_test_output.py Task1/Task1_1/Output/comments_count-24-05-2019-22:22:14.csv test_output.cvs
```

In case of running tests for Task1, they can be run with any configuration of 
database as they are database independent so you can just refill Kafka.

In case of running tests for Task3, they can be run with only with configuration of 
database for 1k dataset so you should refill both Kafka and database with 1k dataset.

Tests for Task2 we could not produce. We tried to manually do all the computation
required only for one pair of users and that took more than 1 hour with many 
mistakes on the way. So creating syntethic data was simply not the option and
also verifying coupld of produced output has the same issue and it is not an option.
