#!/bin/bash
echo "Setting up SQL configuration..."
mysql -u root -p < setup.sql
echo "SQL config done"
