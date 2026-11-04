#!/bin/sh
# Start nginx-thrift (OpenResty) in the foreground.
#
# nginx resolves the service names in the Lua scripts with its own resolver,
# which must be given an address: use the container's first nameserver, so the
# same nginx.conf works under Docker Compose (127.0.0.11) and Kubernetes
# (the cluster DNS). On Kubernetes, fqdn_suffix (for example
# ".<namespace>.svc.cluster.local") makes the names fully qualified.
set -e
conf=/usr/local/openresty/nginx/conf
nameserver=$(awk '$1 == "nameserver" { print $2; exit }' /etc/resolv.conf)
if [ -n "$nameserver" ]; then
  echo "resolver $nameserver valid=10s ipv6=off;" > "$conf/resolver.conf"
fi
exec /usr/local/openresty/bin/openresty -g 'daemon off;'
