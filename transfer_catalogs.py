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

from modify_catalogs import REMOTE_AGGREGATIONS_DIR


REMOTE_CATALOG_DEST = "/var/lib/tomcat/content/thredds/esacci"


class RemoteCatalogHandler(object):
    def __init__(self, user, server, verbose=False, dry_run=False):
        self.host_spec = "{}@{}".format(user, server)
        self.verbose = verbose
        self.dry_run = dry_run

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
        # TODO: Use reinit URL instead of restarting tomcat
        self.remote_command(["service", "tomcat", "restart"])

    def copy_to_server(self, catalog_paths, ncml_paths):
        """
        Copy catalogs and NcML files at the given paths to the remote server
        """
        # Ensure directory to place catalogs in exists on the remote machine
        self.remote_command(["mkdir", "-p", REMOTE_CATALOG_DEST])

        # Ensure a path ends with '/' if it is a directory to get correct
        # behaviour when rsync'ing
        def normalise_path(p):
            return p + "/" if os.path.isdir(p) and not p.endswith("/") else p

        # Build a list of (src, dest) to use with rsync
        transfers = []
        for path in catalog_paths:
            transfers.append((normalise_path(path), REMOTE_CATALOG_DEST))
        for path in ncml_paths:
            transfers.append((normalise_path(path), REMOTE_AGGREGATIONS_DIR))

        for src, dest in transfers:
            self.rsync(src, dest)

        self.reinit_server()

    def delete_from_server(self, catalog_paths, ncml_paths):
        """
        Delete catalogs and NcML files from the remote server
        """
        def prepend_path(prefix):
            return lambda p: os.path.join(prefix, p)

        command = ["rm", "-f", "--"]
        command += list(map(prepend_path(REMOTE_CATALOG_DEST), catalog_paths))
        command += list(map(prepend_path(REMOTE_AGGREGATIONS_DIR), ncml_paths))
        self.remote_command(command)

        self.delete_empty_dirs(REMOTE_CATALOG_DEST)
        self.delete_empty_dirs(REMOTE_AGGREGATIONS_DIR)

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
        return self.retrieve_file(os.path.join(REMOTE_CATALOG_DEST, path))

    def retrieve_ncml(self, path):
        return self.retrieve_file(os.path.join(REMOTE_AGGREGATIONS_DIR, path))


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # General arguments
    parser.add_argument(
        "-s", "--server",
        default="cci-odp-data.ceda.ac.uk",
        help="Hostname of server to transfer catalogs to [default: %(default)s]"
    )
    parser.add_argument(
        "-u", "--user",
        default="root",
        help="Username to connect to CCI server as [default: %(default)s]"
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
        "-c", "--catalog-path",
        default=[],
        action="append",
        help="Path of catalog to copy/delete. In copy mode, this is a local "
             "path to a file or directory. In delete mode, this is the path "
             "relative to the esacci THREDDS root. Can be given multiple times"
    )
    parser.add_argument(
        "-n", "--ncml-path",
        default=[],
        action="append",
        help="As with --catalog-path but for NcML aggregation files"
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
        help="Retrieve content from remote node. -c and -n are interpreted "
             "the way as for 'delete', and retrieve content is written to "
             "stdout"
    )

    args = parser.parse_args(arg_list)

    handler = RemoteCatalogHandler(user=args.user, server=args.server,
                                   verbose=args.verbose, dry_run=args.dry_run)

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


if __name__ == "__main__":
    main(sys.argv[1:])
