# Quickstart

## Installation

`esacci-esgf` provides scripts to interact with the [ESGF
publisher](https://github.com/ESGF/esg-publisher) (version >= 3.5.1), and a
number of python scripts, both of which should be installed via Conda. Separate
environments should be used, since ESGF uses Python 2.7 whereas `esacci-esgf`
uses Python 3.4.5. For example:

```bash
# Get conda (if not already installed)
conda_root="/usr/local/conda"
wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p "$conda_root"
export PATH="${conda_root}/bin:$PATH"

# Create environment for ESGF software (publisher and esgf-prepare)
conda create -n esgf-pub
conda activate esgf-pub
# Install publisher and esgf-prepare... (outside the scope of this documentation)
...
conda deactivate

# Create environment for esacci-esgf
cd /path/to/esacci-esgf/
conda env create -f environment.yml -n esacci-esgf
conda activate esacci-esgf

# Install esacci-esgf python pakage
pip install -e .
conda deactivate
```

(the use of a `$conda_root` variable is for demo purposes and is not strictly
necessary)

These conda environments are activated and deactivated as needed during
publishing, so it should not be necessary to explicitly activate them after
installation.

## Usage

Publishing is done with `scripts/publish.sh`. Since it needs to activate both
the publisher and `esacci-esgf` conda environments, make sure neither are
activated before running (the base environment may or may not be activated).

Make sure the required environment variables are set (see [environment
variables](configuration.md#environment-variables)), and that `esg.ini` and
`esg.esacci.ini` are in place and pointed to by the relevant environment
variables.

Generate a proxy certificate for publishing to Solr (see [generating a
proxy certificate](configuration.md#generating-a-proxy-certificate)).

Run `publish.sh` as follows:

```bash
./scripts/publish.sh /path/to/input.csv
```

See [input formats](input_files.md) for the format of the input CSV.

**Note**: for large datasets publication may take a long time, so it is worth
writing output to files and running `publish.sh` in the background:

```bash
./scripts.push.sh /path/to/input.csv >cci.out 2>cci.err &
```

To remove published datasets, use:

```bash
./scripts/unpublish.sh /path/to/mapfile
```

This also requires a proxy certificate to un-publish from Solr.
