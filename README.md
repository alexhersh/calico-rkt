# Calico Networking for rkt

## Building the plugin
To build the calico-kubernetes plugin, clone this repository and run `make`.  This will build the binary, as well as run the unit tests.  To just build the binary, with no tests, run `make binary`.  To only run the unit tests, simply run `make ut`.

## Using rkt Plugins
The [documentation](https://github.com/coreos/rkt/blob/master/Documentation/networking.md) for Network Plugins in rkt will walk you through the basics of setting up networking in rkt.

## Requirements
* A working [ETCD](https://github.com/coreos/etcd) service
* Currently, Calico is capable of networking rkt containers, but our `calico/node` image does not yet work in rkt. For now, you will need to run our `calico/node` [docker image](https://github.com/projectcalico/calico-docker/blob/master/docs/getting-started/default-networking/Demonstration.md) in parallel to your rkt images.

## Installing
* Move the binary for this plugin to `/usr/lib/rkt/plugins/net/calico`.
* Configure your network with a `*.conf` file in `/etc/rkt/net.d/`. This `conf` file should specify `calico` as `type`. For example,
```
{
    "name": "network-name",
    "type": "calico",
    "ipam": {
        "type": "host-local",
        "subnet": "10.1.0.0/16",
    }
}
```
* When you spin up a node with `rkt run`, specify the `--private-net=network-name` flag to enable Calico Networking