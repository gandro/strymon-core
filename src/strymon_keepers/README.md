Queryable state management library for Timely System
====================================================

Branches
--------

There are two branches in this repository: master and evaluation.

* *master* branch contains the library and sample Keeper implementations.

* *evaluation* branch adds some instrumentation to Timely System and contains
 helper programs and scripts for running experiments. Its README also contains
 a list of steps needed to regenerate the plots from my thesis.


Directory structure
-------------------

This repository contains code of the Keepers library, implementation of a few
sample Keepers, modified code of Timely System and links to repository required
for Topology Keeper to work. They are structured in folders in the following
way:

### `./src`

This folder contains the Keepers library. It has both the Keeper and Keeper's
client libraries.

The client library is in `client.rs` and the main Keeper framework is in folder
`keepers/`.

### `./timely_system`

This folder contains the modified Timely System repository. It was included as
subtree and then never merged back upstream.

The code was modified to make the coordinator aware of Keepers and to fix a few
small bugs.

### `./keepers`

This folder includes the actual Keeper implementations, together with sample clients.

Each Keeper haas its own Cargo package. They are included as the member of the
workspace defined in the root of the repo.

### `./assets`

This folder contains some raw data files for the Keeper implementations to run with.

So, for example, there is the `ulysses.txt` file for running the Word Count
Keeper and a few files containing network topologies for the Topology Keeper.

### `./routing-algorithms`

This is a git submodule, pointing to `routing-algorithms` repo. It is needed
for Topology Keeper, that uses the Topology structure from there.

Cloning
-------
To build Topology Keeper you need to initialize
`routing-algorithms` directory, which is a git submodule. To do so run the
following commands (from the root of the repository):
```
cd routing-algorithms

git submodule init

git submodule update
```

Building
--------

To build the library you can simply run:

```
cargo build
```

Since it will work only with the modified version of Timely System, you also
need to build and run it. The instructions for doing this are in the
`README.md` of `./timely_system/` folder.

If you want to also build the Keeper implementations, you can either enter
their respective directories and run `cargo build` in each of them, or, to
simply build them all, run this in the root directory:

```
cargo build --all
```

Note, that to build the Topology Keeper, you first need to imitialize the
`./routing-algorithms` repo (see the [Cloning](#Cloning) section).

Each of the Keepers and their clients may require some command line arguments
to be run properly, in such case you can find them out by running the program
with `--help` first.


Using the library
-----------------
To use this library you must use the same version of the timely as the
library, otherwise a lot of confusing errors are going to show up.

Keeper implementations are a good example of how to use the library. You first
need to list it as a dependency in `Cargo.toml`, for example in following way:

```
[dependencies.timely-keepers]
path = "<path-to-root-of-the-repository>"
```

And then you need to include it in your Rust code with:

```
extern crate timely_keepers;
```


Timely System
-------------
timely_system was added by running the following commands:
```
git remote add timely_system ssh://vcs-user@code.systems.ethz.ch:8006/diffusion/248/master-thesis-sebastian-wicki.git
git subtree add --prefix=timely_system --squash timely_system master
```

It was later modified, mostly by adding Keepers to the coordinator and fixing small bugs.
