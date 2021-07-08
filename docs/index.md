easy-manage-deposit
===================

Manage DANS deposit directories.

SYNOPSIS
--------

    easy-manage-deposit report full [-a, --age <n>] [-m, --datamanager <uid>] [<depositor>]
    easy-manage-deposit report summary [-a, --age <n>] [-m, --datamanager <uid>] [<depositor>]
    easy-manage-deposit report error [-a, --age <n>] [-m, --datamanager <uid>] [<depositor>]
    easy-manage-deposit report raw [<location>]
    easy-manage-deposit clean [-d, --data-only] [-s, --state <state>] [-k, --keep <n>]  \
            [-l, --new-state-label <state>] [-n, --new-state-description <description>] \
            [-f, --force] [-o, --output] [--do-update] [<depositor>]
    easy-manage-deposit sync-fedora-state <easy-dataset-id>
    easy-manage-deposit run-service

ARGUMENTS
--------

     Options:
            -h, --help      Show help message
            -v, --version   Show version of this program
          
          Subcommand: report
            -h, --help   Show help message
          
          Subcommand: report full - creates a full report for a depositor and/or datamanager
            -a, --age  <arg>           Only report on the deposits that are less than n
                                       days old. An age argument of n=0 days corresponds
                                       to 0<=n<1. If this argument is not provided, all
                                       deposits will be reported on.
            -m, --datamanager  <arg>   Only report on the deposits that are assigned to
                                       this datamanager.
            -h, --help                 Show help message
          
           trailing arguments:
            depositor (not required)
          ---
          
          Subcommand: report summary - creates a summary report for a depositor and/or datamanager
            -a, --age  <arg>           Only report on the deposits that are less than n
                                       days old. An age argument of n=0 days corresponds
                                       to 0<=n<1. If this argument is not provided, all
                                       deposits will be reported on.
            -m, --datamanager  <arg>   Only report on the deposits that are assigned to
                                       this datamanager.
            -h, --help                 Show help message
          
           trailing arguments:
            depositor (not required)
          ---
          
          Subcommand: report error - creates a report displaying all failed, rejected and invalid deposits for a depositor and/or datamanager
            -a, --age  <arg>           Only report on the deposits that are less than n
                                       days old. An age argument of n=0 days corresponds
                                       to 0<=n<1. If this argument is not provided, all
                                       deposits will be reported on.
            -m, --datamanager  <arg>   Only report on the deposits that are assigned to
                                       this datamanager.
            -h, --help                 Show help message
          
           trailing arguments:
            depositor (not required)
          ---
          
          Subcommand: report raw - creates a report containing all content of deposit.properties without inferring any properties
            -h, --help   Show help message
          
           trailing arguments:
            location (required)
          ---
          Subcommand: clean - removes deposit with specified state
            -d, --data-only                      If specified, the deposit.properties and
                                                 the container file of the deposit are not
                                                 deleted
                --do-update                      Do the actual deleting of deposits and
                                                 updating of deposit.properties
            -f, --force                          The user is not asked for a confirmation
            -k, --keep  <arg>                    The deposits whose ages are greater than
                                                 or equal to the argument n (days) are
                                                 deleted. An age argument of n=0 days
                                                 corresponds to 0<=n<1. (default = -1)
            -n, --new-state-description  <arg>   The state description in
                                                 deposit.properties after the deposit has
                                                 been deleted
            -l, --new-state-label  <arg>         The state label in deposit.properties
                                                 after the deposit has been deleted
            -o, --output                         Output a list of depositIds of the
                                                 deposits that were deleted
            -s, --state  <arg>                   The deposits with the specified state
                                                 argument are deleted
            -h, --help                           Show help message
            
           trailing arguments:
            depositor (not required)
          ---
          
          Subcommand: sync-fedora-state - Syncs a deposit with Fedora, checks if the deposit is properly registered in Fedora and updates the deposit.properties accordingly
            -h, --help   Show help message
          
           trailing arguments:
            easy-dataset-id (required)   The dataset identifier of the deposit which
                                         deposit.properties are being synced with Fedora
          ---

          Subcommand: delete-properties - Deletes selected properties from the databse
            -l, --location  <arg>   Only delete deposits from this location (one of:
            SWORD2, INGEST_FLOW, INGEST_FLOW_ARCHIVED
            -h, --help              Show help message
        
            trailing arguments:
              uuid (not required)   Only load this deposit
          ---
        
          Subcommand: load-properties - (Re-)loads the deposit properties into the database, overwriting the current records
            -l, --location  <arg>   Only load deposits from this location (one of: SWORD2,
               INGEST_FLOW, INGEST_FLOW_ARCHIVED
            -h, --help              Show help message
        
            trailing arguments:
              uuid (not required)   Only load this deposit
          ---

          Subcommand: run-service - Starts EASY Manage Deposit as a daemon
          -h, --help   Show help message
          ---


DESCRIPTION
-----------
This module provides commands to manage [DANS deposit directories](https://dans-knaw.github.io/dd-dans-deposit-to-dataverse/deposit-directory/) 
which are the unit of work for the automated deposit pipelines of EASY (legacy) and the DANS Data Stations.

EXAMPLES
--------

     easy-manage-deposit report error someUserId
     easy-manage-deposit report full someUserId
     easy-manage-deposit report summary someUserId
     easy-manage-deposit report error -a 0 someUserId
     easy-manage-deposit report full -a 0 someUserId
     easy-manage-deposit report summary --age 2 someUserId
     easy-manage-deposit clean someUserId
     easy-manage-deposit clean --data-only --state <state> --keep <n> someUserId
     easy-manage-deposit retry someUserId


INSTALLATION AND CONFIGURATION
------------------------------
Currently this project is built as an RPM package for RHEL7/CentOS7 and later. The RPM will install the binaries to
`/opt/dans.knaw.nl/easy-manage-deposit` and the configuration files to `/etc/opt/dans.knaw.nl/easy-manage-deposit`.

To install the module on systems that do not support RPM, you can copy and unarchive the tarball to the target host.
You will have to take care of placing the files in the correct locations for your system yourself. For instructions
on building the tarball, see next section.


BUILDING FROM SOURCE
--------------------

Prerequisites:

* Java 8 or higher
* Maven 3.3.3 or higher
* RPM

Steps:

        git clone https://github.com/DANS-KNAW/easy-manage-deposit.git
        cd easy-manage-deposit
        mvn install

If the `rpm` executable is found at `/usr/local/bin/rpm`, the build profile that includes the RPM
packaging will be activated. If `rpm` is available, but at a different path, then activate it by using
Maven's `-P` switch: `mvn -Pprm install`.

Alternatively, to build the tarball execute:

    mvn clean install assembly:single
