#!/bin/bash
sudo daemonize -e /tmp/rvfs.log -o /tmp/rvfs.log -u root /sbin/mount.fuse.rvfs "$@"
exit

