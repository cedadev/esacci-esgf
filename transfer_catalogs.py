#!/usr/bin/env python3
"""
Transfer THREDDS catalogs and NcML aggregations to another THREDDS server.
"""
import sys
import os
import subprocess
import argparse

from modify_catalogs import REMOTE_AGGREGATIONS_DIR


REMOTE_THREDDS_CONTENT_DIR = "/var/lib/tomcat/content/thredds"
REMOTE_CATALOG_DEST = os.path.join(REMOTE_THREDDS_CONTENT_DIR, "esacci")


class CatalogTransferer(object):
    def __init__(self, user, host, catalog_dir, ncml_dir, verbose=False,
                 delete=False):
        self.host_spec = "{}@{}".format(user, host)

        # Ensure directories end in '/' to get correct behaviour when rsync'ing
        if not catalog_dir.endswith("/"):
            catalog_dir += "/"
        if not ncml_dir.endswith("/"):
            ncml_dir += "/"

        self.catalog_dir = catalog_dir
        self.ncml_dir = ncml_dir
        self.verbose = verbose
        self.delete = delete

    def run_command(self, args):
        """
        Run an external command locally and discard std{out,err}. Raises an
        exception if the child process returns a non-zero exit status.
        """
        if self.verbose:
            print(" ".join(args))
        subprocess.check_call(args, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)

    def rsync(self, src, dest):
        """
        Use rsync to copy src to dest on the remote machine
        """
        remote_dest = "{}:{}".format(self.host_spec, dest)
        args = ["-a", src, remote_dest]

        if self.delete:
            args.insert(0, "--delete")

        self.run_command(["rsync"] + args)

    def remote_command(self, args):
        """
        Run a command on the remote machine via SSH
        """
        self.run_command(["ssh", self.host_spec, "--"] + args)

    def do_all(self):
        # Ensure directory to place catalogs in exists on the remote machine
        self.remote_command(["mkdir", "-p", REMOTE_CATALOG_DEST])

        # Construct src/dest paths for root catalog (which links to 'top level'
        # catalog)
        local_root_cat = os.path.join(os.path.dirname(__file__), "static",
                                      "catalog.xml")
        remote_root_cat = os.path.join(REMOTE_THREDDS_CONTENT_DIR,
                                       "catalog.xml")

        transfers = [
            (self.catalog_dir, REMOTE_CATALOG_DEST),
            (self.ncml_dir, REMOTE_AGGREGATIONS_DIR),
            (local_root_cat, remote_root_cat),
        ]
        for src, dest in transfers:
            self.rsync(src, dest)

        # Restart tomcat
        self.remote_command(["service", "tomcat", "restart"])


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "cci_server",
        nargs="?",
        default="cci-odp-data.ceda.ac.uk",
        help="Hostname of server to transfer catalogs to [default: %(default)s]"
    )
    parser.add_argument(
        "-u", "--user",
        default="root",
        help="Username to connect to CCI server as [default: %(default)s]"
    )
    parser.add_argument(
        "-c", "--catalog-dir",
        required=True,
        help="Directory containing catalogs"
    )
    parser.add_argument(
        "-n", "--ncml-dir",
        required=True,
        help="Directory containing NcML aggregations"
    )
    parser.add_argument(
        "-d", "--delete",
        action="store_true",
        default=False,
        help="Delete remote catalogs/aggregations not present in the input "
             "directories"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="Print rsync/ssh commands as they are run"
    )

    args = parser.parse_args(arg_list)
    txer = CatalogTransferer(args.user, args.cci_server, args.catalog_dir,
                             args.ncml_dir, verbose=args.verbose,
                             delete=args.delete)
    txer.do_all()


if __name__ == "__main__":
    main(sys.argv[1:])
