Queryable state management library for Timely System
====================================================

Anyone using this library must use the same version of the timely as the library, otherwise a lot of confusing errors is going to happen.

Cloning
-------
To build some parts of the repository you need to also initialize
`routing-algorithms` directory, which is a git submodule. To do so run the
following commands (from the root of the directory):
```
cd routing-algorithms

git submodule init

git submodule update
```

Misc
----
How timely_system was added:
```
git remote add timely_system ssh://vcs-user@code.systems.ethz.ch:8006/diffusion/248/master-thesis-sebastian-wicki.git
git subtree add --prefix=timely_system --squash timely_system master
```
