# Data Stream Processing and Analytics

## Requirements:
Before running the project install the following software:

*  The project requires MySQL to be installed on the machine running it.

*  The project requires Kafka to be installed on the machine running it.

## Initial Configuration:
After the MySQL is installed, database and user are needed to be created. 
This can be done by runnnig the setup.sh bash script using command:
```
sudo ./setup.sh
```
Creating tables in the database, populating them and filling Kafka is performed **ONLY** on the first run of the project. 
If the project should be ran with different dataset, insert -delete argument.

## Running the project:
Before each run of the project, make sure Kafka is also started. You can do it with these commands:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```


