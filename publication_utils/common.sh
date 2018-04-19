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
    run_in_conda_env "$PUB_CONDA_ROOT" esgf-pub $@
}

# Run a command in the esgf_wms conda enviroment
cci_env() {
    run_in_conda_env "$ESACCI_CONDA_ROOT" esgf_wms $@
}

# Check we can SSH to the remote server
ssh_check() {
    log "checking SSH access to ${REMOTE_TDS_HOST}..."
    ssh ${REMOTE_TDS_USER}@${REMOTE_TDS_HOST} ls > /dev/null 2>&1 || \
        die "cannot SSH to $REMOTE_TDS_HOST"
}

# Check required environment variables are set
[[ -n "$INI_ROOT" ]]          || die '$INI_ROOT not set'
[[ -n "$PUB_CONDA_ROOT" ]]    || die '$PUB_CONDA_ROOT not set'
[[ -n "$ESACCI_CONDA_ROOT" ]] || die '$ESACCI_CONDA_ROOT not set'
[[ -n "$CATALOG_DIR" ]]       || die '$CATALOG_DIR not set'
[[ -n "$NCML_DIR" ]]          || die '$NCML_DIR not set'

PROJ="esacci"
REMOTE_TDS_HOST="cci-odp-data.ceda.ac.uk"
REMOTE_TDS_USER="root"
INI_DIR="${INI_ROOT}/cci-odp-data"
INI_FILE="${INI_DIR}/esg.ini"
