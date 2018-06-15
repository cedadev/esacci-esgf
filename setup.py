from setuptools import setup, find_packages

requirements = [
    "cached-property==1.3.0",
    "psycopg2-binary==2.7.4",
    "pysolr==3.7.0",
    "requests==2.18.4",
]

setup(
    name="esacci_esgf",
    version="0.0.1",
    description="Scripts to help publish CCI data using the ESGF publisher "
                "and a standalone TDS server",
    packages=find_packages(),
    install_requires=requirements,
    extras_require={
        "test": ["pytest"]
    },
    entry_points={
        "console_scripts": [
            "get_catalog_path=esacci_esgf.get_catalog_path:main",
            "get_catalogs=esacci_esgf.get_catalogs:main",
            "make_mapfiles=esacci_esgf.input.make_mapfiles:main",
            "merge_csv_json=esacci_esgf.input.merge_csv_json:main",
            "modify_catalogs=esacci_esgf.modify_catalogs:main",
            "modify_solr_links=esacci_esgf.modify_solr_links:main",
            "parse_esg_ini=esacci_esgf.input.parse_esg_ini:main",
            "remove_key=esacci_esgf.input.remove_key:main",
            "transfer_catalogs=esacci_esgf.transfer_catalogs:main",
        ]
    }
)
