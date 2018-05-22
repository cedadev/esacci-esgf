#!/usr/bin/env python3
"""
Parse an ESGF ini config file and extract a value
"""
import sys
import argparse
import configparser
from urllib.parse import urlparse


class EsgIniParser(object):
    @classmethod
    def get_host_from_url(cls, url):
        """
        Extract the hostname from a URL
        """
        return urlparse(url).netloc

    @classmethod
    def get_value(cls, esg_ini, key):
        """
        Read the ini file and extract the required value. 'key' should be a key
        in VALUES_MAPPING
        """
        config = configparser.ConfigParser()
        config.read(esg_ini)
        func = VALUES_MAPPING[key]
        return func(config)

    @classmethod
    def get_solr_hostname(cls, config):
        """
        Return the value corresponding to 'rest_service_url' if 'use_rest_api'
        is 'true', or 'hessian_service_url' otherwise.
        """
        url = None
        try:
            assert config.getboolean("DEFAULT", "use_rest_api")
            url = config["DEFAULT"]["rest_service_url"]
            assert url != ""
        except (configparser.NoOptionError, KeyError, AssertionError, ValueError):
            url = config["DEFAULT"]["hessian_service_url"]

        return cls.get_host_from_url(url)

    @classmethod
    def get_by_key(cls, key):
        """
        Return a function that extracts the value corresponding to the given
        key in the 'default' section of the config
        """
        return lambda config: config["DEFAULT"][key]


def compose(f, g):
    """
    Return the composition f o g
    """
    return lambda x: f(g(x))


# Map key name to a callable that accepts a ConfigParser object as its single
# argument and returns a string value
VALUES_MAPPING = {
    "thredds_host": compose(EsgIniParser.get_host_from_url,
                            EsgIniParser.get_by_key("thredds_url")),
    "solr_host": EsgIniParser.get_solr_hostname,
    "thredds_password": EsgIniParser.get_by_key("thredds_password"),
    "thredds_root": EsgIniParser.get_by_key("thredds_root"),
    "publication_db_url": EsgIniParser.get_by_key("dburl")
}


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "esg_ini",
        help="Path to esg.ini"
    )

    parser.add_argument(
        "key",
        choices=VALUES_MAPPING.keys(),
        help="Value to extract from esg.ini"
    )

    args = parser.parse_args(sys.argv[1:])
    print(EsgIniParser.get_value(args.esg_ini, args.key))
