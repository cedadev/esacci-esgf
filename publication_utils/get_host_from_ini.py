#!/usr/bin/env python3
"""
Parse an ESGF ini config file and extract the hostname of the Solr or THREDDS
server.
"""
import sys
import argparse
import configparser
from urllib.parse import urlparse


class HostnameExtractor(object):
    @classmethod
    def get_host_from_url(cls, url):
        return urlparse(url).netloc

    @classmethod
    def get_hostname(cls, esg_ini, service):
        config = configparser.ConfigParser()
        config.read(esg_ini)
        val = SERVICES[service]
        url = config["DEFAULT"][val] if isinstance(val, str) else val(config)
        return cls.get_host_from_url(url)

    @classmethod
    def get_solr_hostname(cls, config):
        """
        Return the value corresponding to 'rest_service_url' if 'use_rest_api'
        is 'true', or 'hessian_service_url' otherwise.
        """
        try:
            assert config.getboolean("DEFAULT", "use_rest_api")
            url = config["DEFAULT"]["rest_service_url"]
            assert url != ""
            return url
        except (configparser.NoOptionError, KeyError, AssertionError, ValueError):
            return config["DEFAULT"]["hessian_service_url"]


# Map service name to key in DEFAULT section that contains a URL, or a
# callable that will take ConfigParser as an argument and return a URL
SERVICES = {
    "thredds": "thredds_url",
    "solr": HostnameExtractor.get_solr_hostname
}


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "esg_ini",
        help="Path to esg.ini"
    )

    parser.add_argument(
        "service",
        choices=SERVICES,
        help="Service to extract the host name for"
    )

    args = parser.parse_args(arg_list)
    print(HostnameExtractor.get_hostname(args.esg_ini, args.service))


if __name__ == "__main__":
    main(sys.argv[1:])
