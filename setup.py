from setuptools import setup, find_packages

requirements = [
    "cached-property==1.3.0",
    "psycopg2-binary==2.7.4",
    "pysolr==3.7.0",
    "requests==2.18.4",
]

setup(
    name="esgf-wms",
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
            "get_catalog_path=esgf_wms.get_catalog_path:main",
            "get_catalogs=esgf_wms.get_catalogs:main",
            "make_mapfiles=esgf_wms.input.make_mapfiles:main",
            "merge_csv_json=esgf_wms.input.merge_csv_json:main",
            "modify_catalogs=esgf_wms.modify_catalogs:main",
            "modify_solr_links=esgf_wms.modify_solr_links:main",
            "parse_esg_ini=esgf_wms.input.parse_esg_ini:main",
            "remove_key=esgf_wms.input.remove_key:main",
            "transfer_catalogs=esgf_wms.transfer_catalogs:main",
        ]
    }
)
