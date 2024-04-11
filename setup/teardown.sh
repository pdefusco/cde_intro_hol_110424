#!/bin/sh

cde_user=$1
max_participants=$2

echo "CDE LOGISTICS HOL DB TEARDOWN INITIATED...."
echo "..."
echo ".."
echo "."
echo "Provided CDE User: "

echo "Upload teardown script to resource log-hol-setup-"$cde_user
cde resource upload --name log-hol-setup-$cde_user --local-path teardown.py
echo "Create teardown job teardown-"$cde_user
cde job create --name teardown-$cde_user --type spark --mount-1-resource log-hol-setup-$cde_user --application-file teardown.py
echo "Run teardown job teardown-"$cde_user
cde job run --name teardown-$cde_user --arg $max_participants
