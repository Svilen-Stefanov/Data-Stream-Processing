# Data Stream Processing and Analytics

## Requirements:
Before running the project install the following software:

*  The project requires MySQL to be installed on the machine running it.

*  The project requires Kafka to be installed on the machine running it.

## Installation and Initial Configuration:
We will not post instructions for Kafka installation as it was part of this course.
However, we will provide instuctions for installing and setting up the MySQL.
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
If you are finding difficult to pick a password, here is a good one: Amazing_123

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
Navigate to data-stream-processing-and-analytics/dspa-project and use command:
```
sudo sh setup.sh
```
Here you will be asked to provide the root user password previously created.
After running this command, only the appropriate database is created but not the tables.
Creating tables in the database, populating them and filling Kafka is performed **ONLY** on the first run of the project. 
If the project should be ran with different dataset, insert -delete argument.

To verify this whole process went correctly run the following commands:
```
sudo mysql -u root -p
use static_database
show tables;
```

You will be asked for your root password again.
If everything is okay it will execute commands without errors and you will see 0 tables.

## Running the project:
Before each run of the project, make sure Kafka is also started. 
You can do it by navigating to Kafka folder and using these commands:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```