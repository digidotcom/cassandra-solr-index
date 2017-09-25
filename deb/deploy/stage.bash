#!/usr/bin/env bash
set -x
# This script basically removes the requirement that hostname, deployment server, repository
# name information is part of the source project. We don't want to have to build a new project
# when changes occur to that.  It is typically called by a bamboo build.

# These are the keys on the local system that are used to connect
# to the target deployment server as the freight user
# This script is usually run from a bamboo server. 
export FREIGHT_KEY=~/freight-keys/freight.key

usage() {
	echo "stage <deploy-server> <repo-name>"
	echo "Stages the debian file in the target directory to the deploy server"
	echo "indicated using ansible and the freight user"
	echo "Requires the freight keys are $FREIGHT_KEY"
	exit 1 
}

if [ "$1" = "-h" ]; then
  usage;
fi


export DEPLOY=$1
shift
export REPO=$1
shift

if [ "x$DEPLOY" = "x" ]; then
  usage;
fi

if [ "x$REPO" = "x" ]; then
  usage;
fi

# No need to parameterize these values yet
export DIST=precise
# This is the ansible configuration we're going to run, create the host file automatically 
export FREIGHT_PLAYBOOK=deb/deploy/stage_freight.yml
export HOSTS_FILE=ansible_hosts

# Create the host file given the name of the deploy server and the repo 
echo -e "[apt]\n$DEPLOY\n\n[apt:vars]\nrepo_name=$REPO" > $HOSTS_FILE

# The Debian file should be present in the target directory
# Find the debian file used by this deployment, and return
# The fully qualified pathname.
export DEBSRCLOCATION=$(find `pwd`/target -name '*.deb')

if [ "x$DEBSRCLOCATION" = "" ]; then
   echo "Can't find the debian to deploy in the target directory"
   exit 1
fi

# Trim off all path components and just get the filename
export DEBFILE=`echo $DEBSRCLOCATION | sed -e 's|.*/||g'`
# Trim off all version information and just get the package name
export DEBPKG=`echo $DEBFILE | cut -f1 -d _`

# Before we run, lets just make sure things are configured correctly from an authorization perspective
if [ -e "$FREIGHT_KEY" ]; then
  :
else
  echo "Couldn't find $FREIGHT_KEY file"
  exit 1 
fi

echo "Staging Deb in Freight using the following hosts file"
cat $HOSTS_FILE
ansible-playbook -vv -i $HOSTS_FILE $FREIGHT_PLAYBOOK --private-key=$FREIGHT_KEY --extra-vars="repo_name=$REPO deb_package=$DEBPKG deb_srclocation=$DEBSRCLOCATION deb_filename=$DEBFILE ubuntu_distribution=$DIST"
# Exit with the ansible playbook return code
exit $?

