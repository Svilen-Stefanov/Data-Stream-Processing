#!/bin/bash

NUM_USERS=${1:-10k}
PASSWORD='Amazing_123'
DATABASE=static_database_$NUM_USERS.sql

if mysql -u svilen -p$PASSWORD -h localhost static_database < $DATABASE ; then
	echo "Static_database successfully imported!"
	echo "SQL config done"
else 
	echo "Import failed!"
fi


