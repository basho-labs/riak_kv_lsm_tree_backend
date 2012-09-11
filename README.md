# The SQLite4 LSM Tree Backend for Riak/KV

This storage engine can function as an alternative backend for Basho's Riak/KV.
See also: [SQLite4](http://www.sqlite.org/src4/doc/trunk/www/storage.wiki)

### Configuration options
Put these values in your `app.config` in the `lsm_tree` section

```erlang
 {lsm_tree, [
          {data_root, "./data/lsm_tree"}
         ]},
```

### NOT FOR PRODUCTION USE in Riak/KV clusters

This is an experimental backend, it should not be used for production
deployments.  The goal is to understand the operational features of the
log-structured merge tree written for use in SQLite version 4, nothing else.

### How to deploy LSM Tree as a Riak/KV backend

Deploy `lsm_tree` into a Riak devrel cluster using the `enable-lsm_tree`
script. Clone the `riak` repo, change your working directory to it, and then
execute the `enable-lsm_tree` script. It adds `lsm_tree` as a dependency, runs
`make all devrel`, and then modifies the configuration settings of the
resulting dev nodes to use the lsm_tree storage backend.

1. `git clone git://github.com/basho/riak.git`
1. `mkdir riak/deps`
1. `cd riak/deps`
1. `git clone git://github.com/basho-labs/riak_kv_lsm_tree_backend.git`
1. `cd ..`
1. `./deps/riak_kv_lsm_tree_backend/enable-lsm_tree`
