# 1.5.0

Despite that the major part of the version was bumped up, this release does not include
any breaking changes, only improvements and additions.

* Addition of a batch concept with `BacthId` which:

  * makes it possible for a `Scheduler` to be "resumable" with the help of `waitForBatch`,
    `waitForBatch_` and `waitForBatchR`
  * allows to cancel batches prematurely while keeping the results and not terminating the
    scheduler itself. This can be done with new functions: `cancelBatch` and
    `cancelBatchWith`. In order to check for batch status `getCurrentBatchId` and
    `hasBatchFinished` functions can be used.

* Addition of `GlobalScheduler` that can be reused throughout the codebase, thus reducing
  initialization overhead.

# 1.4.2

* Add `withTrivialScheduler`
* Add `Results` data type as well as corresponding functions:
  * `withSchedulerR`
  * `withSchedulerWSR`
  * `withTrivialSchedulerR`

# 1.4.1

* Add functions: `replicateWork`

# 1.4.0

* Worker id has been promoted from `Int` to a `newtype` wrapper `WorkerId`.
* Addition of `SchedulerWS` and `WorkerStates` data types. As well as the
  related `MutexException`
* Functions that came along with stateful worker threads:
  * `initWorkerStates`
  * `workerStatesComp`
  * `scheduleWorkState`
  * `scheduleWorkState_`
  * `withSchedulerWS`
  * `withSchedulerWS_`
  * `unwrapSchedulerWS`
* Made internal modules accessible, but invisible.

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
