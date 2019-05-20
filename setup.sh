#!/bin/bash

NUM_USERS=${1:-1k}
PASSWORD='Amazing_123'
DATABASE=Database/static_database_$NUM_USERS.sql
EXPORT=${2:-false}

echo "Setting up SQL configuration..."

if mysql -u svilen -p < Database/setup.sql ; then
	echo "SQL config done"

	if [ "$EXPORT" = true ] ; then
		if mysqldump -u svilen$PASSWORD -p static_database > $DATABASE ; then
			echo "Exporting static_database succeeded!"
		else
			 echo "Export failed!"
		fi
	else
		if mysql -u svilen -p$PASSWORD -h localhost static_database < $DATABASE ; then
			echo "Static_database successfully imported!"
			echo "SQL config done"
		else 
			echo "Import failed!"
		fi
	fi
else 
	echo "SQL config failed!"
fi


