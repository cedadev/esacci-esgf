#!/usr/bin/env python3
"""
Manage THREDDS catalogs and NcML aggregations on a remote THREDDS server.
Supports copying content from the local machine, deleting content on the remote
machine, and retrieving content from the remote machine.
"""
import sys
import os
import subprocess
import argparse

import requests


class RemoteCatalogHandler(object):
    def __init__(self, user, server, remote_catalog_dir, remote_agg_dir,
                 verbose=False, dry_run=False, reinit=False,
                 thredds_credentials=None):
        self.hostname = server
        self.host_spec = "{}@{}".format(user, self.hostname)
        self.remote_agg_dir = remote_agg_dir
        self.remote_catalog_dir = remote_catalog_dir
        self.verbose = verbose
        self.dry_run = dry_run
        self.reinit = reinit
        self.thredds_credentials = thredds_credentials

    def run_command(self, args):
        """
        Run an external command locally and return stdout contents. Raises an
        exception if the child process returns a non-zero exit status
        """
        if self.verbose:
            print(" ".join(args))
        if self.dry_run:
            return None

        output = subprocess.check_output(args, stderr=subprocess.DEVNULL)
        return output.decode()

    def rsync(self, src, dest):
        """
        Use rsync to copy src to dest on the remote machine
        """
        remote_dest = "{}:{}".format(self.host_spec, dest)
        args = ["-a", src, remote_dest]
        self.run_command(["rsync"] + args)

    def remote_command(self, args):
        """
        Run a command on the remote machine via SSH
        """
        return self.run_command(["ssh", self.host_spec, "--"] + args)

    def reinit_server(self):
        """
        Re-initialise THREDDS on the remote server
        """
        url = ("http://{hostname}/thredds/admin/debug/?catalogs/reinit"
               .format(hostname=self.hostname))
        response = requests.get(url, auth=self.thredds_credentials)
        if response.status_code != 200:
            sys.stderr.write("WARNING: THREDDS reinit failed with status code "
                             "{}\n".format(response.status_code))

    def copy_to_server(self, catalog_paths, ncml_paths):
        """
        Copy catalogs and NcML files at the given paths to the remote server
        """
        # Ensure directory to place catalogs in exists on the remote machine
        self.remote_command(["mkdir", "-p", self.remote_catalog_dir])

        # Ensure a path ends with '/' if it is a directory to get correct
        # behaviour when rsync'ing
        def normalise_path(p):
            return p + "/" if os.path.isdir(p) and not p.endswith("/") else p

        # Build a list of (src, dest) to use with rsync
        transfers = []
        for path in catalog_paths:
            transfers.append((normalise_path(path), self.remote_catalog_dir))
        for path in ncml_paths:
            transfers.append((normalise_path(path), self.remote_agg_dir))

        for src, dest in transfers:
            self.rsync(src, dest)

        if self.reinit:
            self.reinit_server()

    def delete_from_server(self, catalog_paths, ncml_paths):
        """
        Delete catalogs and NcML files from the remote server
        """
        def prepend_path(prefix):
            return lambda p: os.path.join(prefix, p)

        command = ["rm", "-f", "--"]
        command += list(map(prepend_path(self.remote_catalog_dir), catalog_paths))
        command += list(map(prepend_path(self.remote_agg_dir), ncml_paths))
        self.remote_command(command)

        self.delete_empty_dirs(self.remote_catalog_dir)
        self.delete_empty_dirs(self.remote_agg_dir)

        if self.reinit:
            self.reinit_server()

    def delete_empty_dirs(self, root_dir):
        """
        Find and delete empty directory trees on the remote server under the
        given root directory
        """
        # Note that the root dir itself will not be deleted
        self.remote_command(["find", root_dir, "-mindepth", "1", "-type", "d",
                             "-empty", "-delete"])

    def retrieve_file(self, path):
        """
        Return the contents of a file on the remote server as a string
        """
        return self.remote_command(["cat", path])

    def retrieve_catalog(self, path):
        return self.retrieve_file(os.path.join(self.remote_catalog_dir, path))

    def retrieve_ncml(self, path):
        return self.retrieve_file(os.path.join(self.remote_agg_dir, path))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # General arguments
    parser.add_argument(
        "-s", "--server",
        required=True,
        help="Hostname of server to transfer catalogs to"
    )
    parser.add_argument(
        "-u", "--user",
        default="root",
        help="Username to connect to the server as [default: %(default)s]"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="Print rsync/ssh commands as they are run"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Don't actually run commands on remote host (use with --verbose)"
    )
    parser.add_argument(
        "--reinit",
        action="store_true",
        default=False,
        help="Reinitialise THREDDS catalogs after copy or delete operations "
             "[default: %(default)s]"
    )
    parser.add_argument(
        "--thredds-username",
        help="THREDDS admin username to use when calling reinit URL"
    )
    parser.add_argument(
        "--thredds-password",
        help="THREDDS admin password to use when calling reinit URL"
    )
    parser.add_argument(
        "-c", "--catalog-path",
        default=[],
        action="append",
        help="Path of catalog to copy/delete. In copy mode, this is a local "
             "path to a file or directory. In delete mode, this is the path "
             "relative to the remote catalog root directory. Can be given "
             "multiple times"
    )
    parser.add_argument(
        "-n", "--ncml-path",
        default=[],
        action="append",
        help="As with --catalog-path but for NcML aggregation files"
    )
    parser.add_argument(
        "--remote-catalog-dir",
        required=True,
        help="Directory under which catalogs are stored on the server"
    )
    parser.add_argument(
        "--remote-agg-dir",
        default="/usr/local/aggregations/",
        help="Directory under which NcML aggregations are stored on the server"
             " [default: %(default)s]"
    )

    # Add subcommands for copy, delete and retrieve
    subparsers = parser.add_subparsers(
        dest="mode",
        metavar="MODE",
        help="Mode to run in - either copy or delete"
    )
    subparsers.required = True
    subparsers.add_parser(
        "copy",
        help="Copy content to remote node"
    )
    subparsers.add_parser(
        "delete",
        help="Delete content from remote node"
    )
    subparsers.add_parser(
        "retrieve",
        help="Retrieve content from remote node. -c and -n are interpreted in "
             "the same way as for 'delete', and retrieved content is written "
             "to stdout"
    )

    args = parser.parse_args(sys.argv[1:])

    thredds_creds = None
    if args.reinit:
        if not (args.thredds_username and args.thredds_password):
            parser.error("Must give --thredds-username and --thredds-password "
                         "when using --reinit")
        thredds_creds = (args.thredds_username, args.thredds_password)

    handler = RemoteCatalogHandler(user=args.user, server=args.server,
                                   remote_catalog_dir=args.remote_catalog_dir,
                                   remote_agg_dir=args.remote_agg_dir,
                                   verbose=args.verbose, dry_run=args.dry_run,
                                   reinit=args.reinit,
                                   thredds_credentials=thredds_creds)

    if args.mode == "copy":
        handler.copy_to_server(args.catalog_path, args.ncml_path)

    elif args.mode == "delete":
        handler.delete_from_server(args.catalog_path, args.ncml_path)

    elif args.mode == "retrieve":
        if len(args.catalog_path) == 1 and not args.ncml_path:
            contents = handler.retrieve_catalog(args.catalog_path[0])
        elif len(args.ncml_path) == 1 and not args.catalog_path:
            contents = handler.retrieve_ncml(args.ncml_path[0])
        else:
            parser.error("Must specify exactly one catalog or NcML file with "
                         "'retrieve'")
        print(contents)
