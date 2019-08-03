#!/bin/bash

yum -y update

yum -y install net-tools ntp wget lsof unzip tar iptables-services
systemctl enable ntpd && systemctl start ntpd
systemctl disable firewalld && systemctl stop firewalld

iptables --flush INPUT && \
iptables --flush FORWARD && \
service iptables save

setenforce 0
sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config
echo -n "Check the status of SELinux"
getenforce

yum install yum-utils
yum-config-manager --enable rhui-REGION-rhel-server-extras
yum install -y docker
systemctl start docker
systemctl enable docker

docker info | grep "Logging Driver"

vi /etc/sysconfig/docker

# EDIT THE DOCKER CONFIG TO ADD json-file for log-driver
# OPTIONS='--selinux-enabled --log-driver=json-file --signature-verification=false'

systemctl restart docker
systemctl status docker

yum install -y jq

yum -y install unzip tar
curl -Ls public-repo-1.hortonworks.com/HDP/cloudbreak/cloudbreak-deployer_2.9.1_$(uname)_x86_64.tgz | sudo tar -xz -C /bin cbd
cbd --version

mkdir /opt/cloudbreak-deployment
pwd
cd /opt/cloudbreak-deployment

# go to the folder /opt/cloudbreak-deployment, and creaet a file called "Profile"
# Contents of the Profile file are:
#
#
#
#
# Run below commands in the folder where Profile was exists
# $ cbd generate
# $ cbd pull-parallel
# $ cbd start

##########

# Start Posgres database on Azure PaaS or Install POSTGRES on an Azure VM
# Ensure that client machine can connect to the Postgresql server. If on Azure, you can go to Connection Security, and add the client IP address
# to allowed IPs to connect to Postgresql server

Install postgresql on a remote machine
$ yum install postgresql
$ psql "host=tdf-sb-postgres.postgres.database.azure.com port=5432 dbname=postgres user=pgadmin@tdf-sb-postgres password=hadoop@cloudera1 sslmode=require"

postgres=> create database cbdb;
CREATE DATABASE
postgres=> create database periscopedb;
CREATE DATABASE
postgres=> create database uaadb;
CREATE DATABASE
postgres=> create user cbdadmin with encrypted password 'hadoop@cloudera1';
CREATE ROLE
postgres=> GRANT ALL PRIVILEGES ON DATABASE cbdb to cbdadmin;
GRANT
postgres=> GRANT ALL PRIVILEGES ON DATABASE periscopedb to cbdadmin;
GRANT
postgres=> GRANT ALL PRIVILEGES ON DATABASE uaadb to cbdadmin;
GRANT
postgres=> GRANT CONNECT ON DATABASE cbdb TO cbdadmin;
GRANT
postgres=> GRANT CONNECT ON DATABASE uaadb TO cbdadmin;
GRANT
postgres=> GRANT CONNECT ON DATABASE periscopedb TO cbdadmin;
GRANT
postgres=>
