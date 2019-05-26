#!/bin/bash

NUM_USERS=${1:-1k}
PASSWORD='Amazing_123'
DATABASE=static_database_$NUM_USERS.sql

if mysqldump -u svilen -p$PASSWORD static_database > $DATABASE ; then
	echo "Exporting static_database succeeded!"
else
	 echo "Export failed!"
fi


