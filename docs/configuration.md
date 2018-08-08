# Configuration

## Environment Variables

`publish.sh` and `unpublish.sh` require several environment variables to be
set:

| Name                 | Description |
| -------------------- | ----------- |
| `INI_DIR`            | directory containing ESGF ini config files. This directory should contain `esg.ini` and `esg.esacci.ini` |
| `MAPFILES_DIR`       | directory under which to write mapfiles |
| `CATALOG_DIR`        | directory to write modified THREDDS catalogs to |
| `NCML_DIR`           | directory to write NcML aggregations to |
| `CONDA_ROOT`         | conda installation directory |
| `PUB_CONDA_ENV`      | name of the conda environment used to run the ESGF publisher |
| `ESACCI_CONDA_ENV`   | name of the conda environment used to run the scripts from this repository (default: `esacci-esgf`) |
| `REMOTE_TDS_USER`    | user to SSH/rsync to the remote THREDDS host as (default: `root`) |
| `REMOTE_CATALOG_DIR` | directory in which to store modified THREDDS catalogs on the remote THREDDS host (default: `/var/lib/tomcat/content/thredds/esacci`) |
| `REMOTE_NCML_DIR`    | directory in which to store NcML aggregations on the remote THREDDS host (default: `/usr/local/aggregations/`) |
| `CERT_FILE`          | path to the certificate file used to authenticate when publishing to Solr (default: `~/.globus/certificate-file`) |

## Generating a user certificate

A user certificate is required to authenticate when publishing to Solr:

```bash
mkdir -p ~/.globus
myproxy-logon -l <CEDA username> -s slcs1.ceda.ac.uk -o ~/.globus/certificate-file -b -t 72
```

(The `-b` flag downloads trustroots to `~/.globus` and only needs to be used
the first time a certificate is generated)

## esg.ini configuration

Besides normal `esg.ini` configuration, the only requirement is that the name
in `thredds_dataset_roots` for the CCI data root must be `esg_esacci`;
e.g.:

```
thredds_dataset_roots =
    esg_esacci | /neodc/esacci/
```

The project ini `esg.esacci.ini` is not used (but it must be correct for the
CCI data in order to publish).
