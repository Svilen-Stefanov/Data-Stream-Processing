#!/bin/bash

NUM_USERS=${1:-10k}
PASSWORD='Amazing_123'
DATABASE=Database/static_database_$NUM_USERS.sql
EXPORT=${2:-false}

echo "Setting up SQL configuration..."

if mysql -u svilen -p < Database/setup.sql ; then
	if mysql -u svilen -p$PASSWORD -h localhost static_database < $DATABASE ; then
		echo "Static_database successfully imported!"
		echo "SQL config done"
	else 
		echo "Import failed!"
	fi
else 
	echo "SQL config failed!"
fi


