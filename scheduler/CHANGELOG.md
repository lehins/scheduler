# 1.3.0

* Make sure internal `Scheduler` accessor functions are no longer exported, they only
  cause breakage.
* Make sure number of capabilities does not change through out the program execution, as
  far as `scheduler` is concerned.

# 1.2.0

* Addition of `scheduleWorkId` and `scheduleWorkId_`

# 1.1.0

* Add functions: `replicateConcurrently` and `replicateConcurrently_`
* Made `traverseConcurrently_` lazy, thus making it possible to apply to infinite lists and other such
  foldables.
* Fix `Monoid` instance for `Comp`
* Addition of `Par'` pattern

# 1.0.0

Initial release.
