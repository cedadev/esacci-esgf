#!/bin/bash

## Script containing functions common to publishing and un-publishing

PROG=`basename $0`

bold=$(tput bold) || bold=""
normal=$(tput sgr0) || normal=""

echo_bold() {
    echo -n "$bold"
    echo $@
    echo -n "$normal"
}

die() {
    echo "$PROG: $@" >&2
    exit 1
}

warn() {
    echo_bold "$PROG: WARNING: $@" >&2
}

log() {
    echo_bold "$PROG: $@"
}

# Activate a conda environment and run a command in a sub-shell
# Usage: run_in_conda_env ENV_ROOT_DIR ENV_NAME command [arg [args...]]
run_in_conda_env() {
    env_root="$1"
    shift
    env_name="$1"
    shift
    (
        . "$env_root/bin/activate" "$env_name" || die "failed to activate conda environment $env_name"
        "$@"
    )
}

# Run a command in the ESGF publisher conda enviroment
esg_env() {
    run_in_conda_env "$CONDA_ROOT" "$PUB_CONDA_ENV" $@
}

# Run a command in the esaci-esgf conda enviroment
cci_env() {
    run_in_conda_env "$CONDA_ROOT" "$ESACCI_CONDA_ENV" $@
}

# Check we can SSH to the remote server
ssh_check() {
    log "checking SSH access to ${REMOTE_TDS_HOST}..."
    ssh ${REMOTE_TDS_USER}@${REMOTE_TDS_HOST} ls > /dev/null 2>&1 || \
        die "cannot SSH to $REMOTE_TDS_HOST"
}

# Usage: certificate_check MIN_HOURS
# Check proxy certificate doesn't expire within MIN_HOURS hours
certificate_test() {
    min_hours=$1
    seconds=$((min_hours * 60 * 60))
    openssl x509 -in "$CERT_FILE" -noout -checkend $seconds 2>/dev/null
}

certificate_check() {
    log "checking certificate expiry time..."
    min_hours=$1
    msg="certificate at '$CERT_FILE' is not valid or expires within $min_hours hours. please renew and try again"
    certificate_test $min_hours || die "$msg"
}

# Usage: dsid_from_mapfile MAPFILE
# This assumes the mapfile only describes a single dataset, as it only looks at
# the first line
dsid_from_mapfile() {
    head -n1 "$1" | cut -d'|' -f1 | sed 's/#/.v/' | sed 's/ //g'
}

# Check required environment variables are set
[[ -n "$INI_DIR" ]]           || die '$INI_DIR not set'
[[ -n "$CONDA_ROOT" ]]        || die '$CONDA_ROOT not set'
[[ -n "$PUB_CONDA_ENV" ]]     || die '$PUB_CONDA_ENV not set'
[[ -n "$CATALOG_DIR" ]]       || die '$CATALOG_DIR not set'
[[ -n "$NCML_DIR" ]]          || die '$NCML_DIR not set'

# Set optional variables
: ${REMOTE_TDS_USER:="root"}
: ${REMOTE_NCML_DIR:="/usr/local/aggregations/"}
: ${REMOTE_CATALOG_DIR:="/var/lib/tomcat/content/thredds/esacci"}
: ${ESACCI_CONDA_ENV:="esacci-esgf"}
: ${CERT_FILE:=~/.globus/certificate-file}

INI_FILE="${INI_DIR}/esg.ini"
PROJ="esacci"

REMOTE_TDS_HOST=`cci_env parse_esg_ini "$INI_FILE" thredds_host` || die "could not get THREDDS host from $INI_FILE"
SOLR_HOST=`cci_env parse_esg_ini "$INI_FILE" solr_host` || die "could not get Solr host from $INI_FILE"
TDS_ADMIN_USER=`cci_env parse_esg_ini "$INI_FILE" thredds_username` || \
    die "could not get THREDDS admin username from $INI_FILE"
TDS_ADMIN_PASSWORD=`cci_env parse_esg_ini "$INI_FILE" thredds_password` || \
    die "could not get THREDDS admin password from $INI_FILE"

REMOTE_TDS_URL="http://${REMOTE_TDS_HOST}/thredds/"
