# scheduler

This is a work stealing scheduler, which is very useful for tasks parallelization.

Whenever you have many actions you'd like to perform in parallel, but would only like to use a few
threads to do the actual computation, this package is for you.

| Language | Travis | Azure | Coveralls |
|:--------:|:------:|:-----:|:---------:|
| ![GitHub top language](https://img.shields.io/github/languages/top/lehins/haskell-scheduler.svg) | [![Travis](https://img.shields.io/travis/lehins/haskell-scheduler/master.svg?label=Linux%20%26%20OS%20X)](https://travis-ci.org/lehins/haskell-scheduler) | [![Build Status](https://dev.azure.com/kuleshevich/haskell-scheduler/_apis/build/status/lehins.haskell-scheduler?branchName=master)](https://dev.azure.com/kuleshevich/haskell-scheduler/_build?definitionId=1&branchName=master) | [![Coverage Status](https://coveralls.io/repos/github/lehins/haskell-scheduler/badge.svg?branch=master)](https://coveralls.io/github/lehins/haskell-scheduler?branch=master) |

| Gihub | Hackage | Nightly | LTS |
|:------|:-------:|:-------:|:---:|
| [`scheduler`](https://github.com/lehins/haskell-scheduler) | [![Hackage](https://img.shields.io/hackage/v/scheduler.svg)](https://hackage.haskell.org/package/scheduler)| [![Nightly](https://www.stackage.org/package/scheduler/badge/nightly)](https://www.stackage.org/nightly/package/scheduler) | [![Nightly](https://www.stackage.org/package/scheduler/badge/lts)](https://www.stackage.org/lts/package/scheduler) |


## QuickStart

A few examples in order to get up and running quickly.

### Schedule simple actions

Work scheduling that does some side effecty stuff and discards the results:

```haskell
interleaveFooBar :: IO ()
interleaveFooBar = do
  withScheduler_ (ParN 2) $ \ scheduler -> do
    putStrLn "Scheduling 1st job"
    scheduleWork scheduler (putStr "foo")
    putStrLn "Scheduling 2nd job"
    scheduleWork scheduler (putStr "bar")
    putStrLn "Awaiting for jobs to be executed:"
  putStrLn "\nDone"
```

In the example above two workers will be created to handle the only two jobs that have been
scheduled. Printing with `putStr` is not thread safe, so the output that you would get with above
function is likely be interleaved:

```haskell
λ> interleaveFooBar
Scheduling 1st job
Scheduling 2nd job
Awaiting for jobs to be executed:
foboar
Done
```

Important to note that only when inner action supplied to the `withScheduler_` exits will the
scheduler start executing scheduled jobs.

### Keeping the results of computation

Another common scenario is to schedule some jobs that produce useful results. In the example below
four works will be spawned off. Due to `ParOn` each of the workers will be pinned to a particular
core.

```haskell
scheduleSums :: IO [Int]
scheduleSums =
  withScheduler (ParOn [1..4]) $ \ scheduler -> do
    scheduleWork scheduler $ pure (10 + 1)
    scheduleWork scheduler $ pure (20 + 2)
    scheduleWork scheduler $ pure (30 + 3)
    scheduleWork scheduler $ pure (40 + 4)
    scheduleWork scheduler $ pure (50 + 5)
```

Despite that the fact that sums are computed in parallel, the results of computation will appear in
the same order they've been scheduled:

```haskell
λ> scheduleSums
[11,22,33,44,55]
```

### Identify workers doing the work

Since version `scheduler-1.2.0` it is possible to identify which worker is doing the
job. This is especially useful for limiting resources to particular workers that should
not be shared between separate threads.

```haskell
λ> let scheduleId = (`scheduleWorkId` (\ i -> threadDelay 100000 >> pure i))
λ> withScheduler (ParOn [4,7,5]) $ \s -> scheduleId s >> scheduleId s >> scheduleId s
[WorkerId {getWorkerId = 0},WorkerId {getWorkerId = 1},WorkerId {getWorkerId = 2}]
λ> withScheduler (ParN 3) $ \s -> scheduleId s >> scheduleId s >> scheduleId s
[WorkerId {getWorkerId = 1},WorkerId {getWorkerId = 2},WorkerId {getWorkerId = 0}]
λ> withScheduler (ParN 3) $ \s -> scheduleId s >> scheduleId s >> scheduleId s
[WorkerId {getWorkerId = 0},WorkerId {getWorkerId = 1},WorkerId {getWorkerId = 2}]
```

### Exceptions

Whenever any of the scheduled jobs result in an exception, all of the workers will be killed and the
exception will get re-thrown in the scheduling thread:

```haskell
infiniteJobs :: IO ()
infiniteJobs = do
  withScheduler_ (ParN 5) $ \ scheduler -> do
    scheduleWork scheduler $ putStrLn $ repeat 'a'
    scheduleWork scheduler $ putStrLn $ repeat 'b'
    scheduleWork scheduler $ putStrLn $ repeat 'c'
    scheduleWork scheduler $ pure (4 `div` (0 :: Int))
    scheduleWork scheduler $ putStrLn $ repeat 'd'
  putStrLn "\nDone"
```

Note, that if there was no exception, printing would never stop.

```haskell
λ> infiniteJobs
aaaaaaaaabcdd*** Exception: divide by zero
```

### Nested jobs

Scheduling actions can themselves schedule actions indefinitely. That of course means that order of
results produced is no longer deterministic, which is to be expected.

```haskell
nestedJobs :: IO ()
nestedJobs = do
  withScheduler_ (ParN 5) $ \ scheduler -> do
    scheduleWork scheduler $ putStr $ replicate 10 'a'
    scheduleWork scheduler $ do
      putStr $ replicate 10 'b'
      scheduleWork scheduler $ do
        putStr $ replicate 10 'c'
        scheduleWork scheduler $ putStr $ replicate 10 'e'
      scheduleWork scheduler $ putStr $ replicate 10 'd'
    scheduleWork scheduler $ putStr $ replicate 10 'f'
  putStrLn "\nDone"
```

The order in which characters appear is important, since it directly relates to the actual order in
which jobs are being scheduled and executed:

* `c`, `d` and `e` characters will always appear after `b`
* `e` will always appear after `c`

```haskell
λ> nestedJobs
abbafbafbafbafbafbafbafbafbaffcdcdcdcdcdcdcdcdcdcdeeeeeeeeee
Done
```

### Nested parallelism

Nothing really prevents you from having a scheduler within a scheduler. Of course, having multiple
schedulers at the same time seems like an unnecessary overhead, which it is, but if you do have a
use case for it, don't make me stop you, it is OK to go that route.

```haskell
nestedSchedulers :: IO ()
nestedSchedulers = do
  withScheduler_ (ParN 2) $ \ outerScheduler -> do
    scheduleWork outerScheduler $ putStr $ replicate 10 'a'
    scheduleWork outerScheduler $ do
      putStr $ replicate 10 'b'
      withScheduler_ (ParN 2) $ \ innerScheduler -> do
        scheduleWork innerScheduler $ do
          putStr $ replicate 10 'c'
          scheduleWork outerScheduler $ putStr $ replicate 10 'e'
        scheduleWork innerScheduler $ putStr $ replicate 10 'd'
    scheduleWork outerScheduler $ putStr $ replicate 10 'f'
  putStrLn "\nDone"
```

Note that the inner scheduler's job schedules a job for the outer scheduler, which is a bit crazy,
but totally safe.

```haskell
λ> nestedSchedulers
aabababababababababbffffffffffcccccccdcdcdcdddededededeeeeee
Done
```

### Single worker schedulers

If we only have one worker, than everything becomes sequential and deterministic. Consider the same
example from before, but with `Seq` computation strategy.

```haskell
nestedSequentialSchedulers :: IO ()
nestedSequentialSchedulers = do
  withScheduler_ Seq $ \ outerScheduler -> do
    scheduleWork outerScheduler $ putStr $ replicate 10 'a'
    scheduleWork outerScheduler $ do
      putStr $ replicate 10 'b'
      withScheduler_ Seq $ \ innerScheduler -> do
        scheduleWork innerScheduler $ do
          putStr $ replicate 10 'c'
          scheduleWork outerScheduler $ putStr $ replicate 10 'e'
        scheduleWork innerScheduler $ putStr $ replicate 10 'd'
    scheduleWork outerScheduler $ putStr $ replicate 10 'f'
  putStrLn "\nDone"
```

No more interleaving, everything is done in the same order each time the function is invoked.

```haskell
λ> nestedSchedulers
aaaaaaaaaabbbbbbbbbbccccccccccddddddddddffffffffffeeeeeeeeee
Done
```

## Premature termination

It is possible to make all of the workers stop whatever they are doing and get either their progress
thus far or simply return an element we were looking for.

For example, we would like to find the 10th letter in the English alphabet in parallel using 8
threads. The way we do it is we schedule 26 tasks, and the first one that will find the letter with
such index will terminate all the workers and return the result:

```haskell
λ> let f sch i c = scheduleWork sch $ if i == 10 then putChar '-' >> terminateWith sch c else threadDelay 1000 >> putChar c >> pure c
λ> withScheduler (ParN 8) (\ scheduler -> zipWithM (f scheduler) [1 :: Int ..] ['a'..'z'])
ab-dec"j"
```



## Benchmarks

It is always good to see some benchmarks. Below is a very simple comparison of:

* [`traverseConcurrently`](https://hackage.haskell.org/package/scheduler-1.0.0/docs/Control-Scheduler.html#v:traverseConcurrently) from `scheduler`
* [`pooledMapConcurrently`](https://hackage.haskell.org/package/unliftio-0.2.10/docs/UnliftIO-Async.html#v:pooledMapConcurrently) from `unliftio`
* [`mapM`](https://hackage.haskell.org/package/streamly-0.6.1/docs/Streamly-Prelude.html#v:mapM) from `streamly`
* [`parMapM`](http://hackage.haskell.org/package/monad-par-extras-0.3.3/docs/Control-Monad-Par-Combinator.html) from `monad-par`
* [`mapConcurrently`](http://hackage.haskell.org/package/async-2.2.1/docs/Control-Concurrent-Async.html#v:mapConcurrently) from `async`
* `traverse` from `base` with
  [`par`](http://hackage.haskell.org/package/parallel-3.2.2.0/docs/Control-Parallel.html#v:par) from
  `parallel`
* Regular sequential `traverse` as a basepoint

Similar functions are used for `replicateM` functionality from the corresponding libraries.

Benchmarked functions `sum` (sum of elements in a list) and `fib` (fibonacci number) are pretty
straightforward, we simply `replicateM` them many times or `mapM` same functions over a
list. Although `scheduler` is already pretty good at it, it does look like there might be some room
for improvement. `pooled*` functions from `unliftio` have the best performance, and I don't think
they can be made any faster for such workloads, but they do not allow submitting more work during
computation, as such nested parallelism while reusing the same workers is impossible, thus
functionally they are inferior.

Benchmarks can be reproduced with `stack bench` inside this repo.

```
scheduler-1.1.0: benchmarks
Running 1 benchmarks...
Benchmark scheduler: RUNNING...
benchmarking replicate/Fib(1000/10000)/scheduler/replicateConcurrently
time                 10.16 ms   (9.235 ms .. 11.09 ms)
                     0.954 R²   (0.899 R² .. 0.982 R²)
mean                 10.42 ms   (9.965 ms .. 11.26 ms)
std dev              1.732 ms   (982.5 μs .. 2.587 ms)
variance introduced by outliers: 76% (severely inflated)

benchmarking replicate/Fib(1000/10000)/unliftio/pooledReplicateConcurrently
time                 8.476 ms   (8.034 ms .. 8.867 ms)
                     0.986 R²   (0.972 R² .. 0.994 R²)
mean                 8.671 ms   (8.410 ms .. 9.090 ms)
std dev              909.4 μs   (634.5 μs .. 1.279 ms)
variance introduced by outliers: 58% (severely inflated)

benchmarking replicate/Fib(1000/10000)/streamly/replicateM
time                 11.90 ms   (11.18 ms .. 12.46 ms)
                     0.976 R²   (0.935 R² .. 0.994 R²)
mean                 12.72 ms   (12.24 ms .. 14.27 ms)
std dev              1.897 ms   (625.2 μs .. 3.881 ms)
variance introduced by outliers: 69% (severely inflated)

benchmarking replicate/Fib(1000/10000)/async/replicateConcurrently
time                 21.16 ms   (19.59 ms .. 22.38 ms)
                     0.982 R²   (0.953 R² .. 0.995 R²)
mean                 22.46 ms   (21.27 ms .. 24.73 ms)
std dev              3.644 ms   (1.499 ms .. 5.373 ms)
variance introduced by outliers: 69% (severely inflated)

benchmarking replicate/Fib(1000/10000)/monad-par/replicateM
time                 31.78 ms   (30.64 ms .. 32.87 ms)
                     0.994 R²   (0.986 R² .. 0.998 R²)
mean                 32.59 ms   (31.86 ms .. 33.98 ms)
std dev              2.290 ms   (1.296 ms .. 3.640 ms)
variance introduced by outliers: 28% (moderately inflated)

benchmarking replicate/Fib(1000/10000)/base/replicateM
time                 31.35 ms   (30.91 ms .. 31.71 ms)
                     0.999 R²   (0.998 R² .. 1.000 R²)
mean                 31.56 ms   (31.06 ms .. 32.64 ms)
std dev              1.440 ms   (674.1 μs .. 2.516 ms)
variance introduced by outliers: 16% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/scheduler/replicateConcurrently
time                 17.10 ms   (16.25 ms .. 17.79 ms)
                     0.984 R²   (0.960 R² .. 0.995 R²)
mean                 17.12 ms   (16.57 ms .. 17.97 ms)
std dev              1.690 ms   (858.3 μs .. 2.444 ms)
variance introduced by outliers: 46% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/unliftio/pooledReplicateConcurrently
time                 15.91 ms   (15.74 ms .. 16.06 ms)
                     0.999 R²   (0.999 R² .. 1.000 R²)
mean                 15.97 ms   (15.87 ms .. 16.15 ms)
std dev              301.2 μs   (198.0 μs .. 421.2 μs)

benchmarking replicate/Sum(1000/1000)/streamly/replicateM
time                 17.51 ms   (17.06 ms .. 17.96 ms)
                     0.997 R²   (0.995 R² .. 0.999 R²)
mean                 17.99 ms   (17.74 ms .. 18.64 ms)
std dev              865.5 μs   (599.3 μs .. 1.295 ms)
variance introduced by outliers: 17% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/async/replicateConcurrently
time                 28.82 ms   (26.60 ms .. 31.82 ms)
                     0.972 R²   (0.948 R² .. 0.987 R²)
mean                 33.20 ms   (31.69 ms .. 34.13 ms)
std dev              2.548 ms   (1.706 ms .. 3.197 ms)
variance introduced by outliers: 28% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/monad-par/replicateM
time                 58.84 ms   (55.71 ms .. 61.47 ms)
                     0.995 R²   (0.991 R² .. 0.999 R²)
mean                 64.34 ms   (62.70 ms .. 65.83 ms)
std dev              3.150 ms   (2.697 ms .. 3.721 ms)
variance introduced by outliers: 15% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/base/replicateM
time                 56.26 ms   (55.82 ms .. 56.74 ms)
                     1.000 R²   (0.999 R² .. 1.000 R²)
mean                 56.70 ms   (56.47 ms .. 57.21 ms)
std dev              618.6 μs   (331.6 μs .. 969.4 μs)

benchmarking map/Fib(2000)/scheduler/traverseConcurrently
time                 12.23 ms   (11.51 ms .. 12.90 ms)
                     0.968 R²   (0.933 R² .. 0.989 R²)
mean                 13.65 ms   (12.87 ms .. 14.50 ms)
std dev              1.985 ms   (1.455 ms .. 2.696 ms)
variance introduced by outliers: 68% (severely inflated)

benchmarking map/Fib(2000)/unliftio/pooledTraverseConcurrently
time                 6.843 ms   (6.435 ms .. 7.201 ms)
                     0.973 R²   (0.939 R² .. 0.992 R²)
mean                 7.223 ms   (6.946 ms .. 8.280 ms)
std dev              1.240 ms   (537.0 μs .. 2.535 ms)
variance introduced by outliers: 82% (severely inflated)

benchmarking map/Fib(2000)/streamly/mapM
time                 21.89 ms   (21.01 ms .. 23.13 ms)
                     0.991 R²   (0.985 R² .. 0.996 R²)
mean                 21.44 ms   (20.16 ms .. 24.26 ms)
std dev              4.518 ms   (1.773 ms .. 8.475 ms)
variance introduced by outliers: 80% (severely inflated)

benchmarking map/Fib(2000)/async/mapConcurrently
time                 37.37 ms   (32.21 ms .. 41.57 ms)
                     0.966 R²   (0.936 R² .. 0.993 R²)
mean                 41.32 ms   (38.94 ms .. 47.96 ms)
std dev              7.065 ms   (2.678 ms .. 12.36 ms)
variance introduced by outliers: 65% (severely inflated)

benchmarking map/Fib(2000)/par/mapM
time                 7.381 ms   (6.934 ms .. 7.848 ms)
                     0.975 R²   (0.953 R² .. 0.987 R²)
mean                 7.519 ms   (7.256 ms .. 8.123 ms)
std dev              1.178 ms   (677.4 μs .. 2.015 ms)
variance introduced by outliers: 78% (severely inflated)

benchmarking map/Fib(2000)/monad-par/mapM
time                 24.57 ms   (23.52 ms .. 25.30 ms)
                     0.994 R²   (0.986 R² .. 0.998 R²)
mean                 26.15 ms   (25.48 ms .. 27.60 ms)
std dev              2.016 ms   (1.267 ms .. 3.175 ms)
variance introduced by outliers: 30% (moderately inflated)

benchmarking map/Fib(2000)/base/mapM
time                 23.79 ms   (22.66 ms .. 24.75 ms)
                     0.993 R²   (0.987 R² .. 0.999 R²)
mean                 24.66 ms   (23.96 ms .. 26.84 ms)
std dev              2.532 ms   (891.4 μs .. 4.701 ms)
variance introduced by outliers: 46% (moderately inflated)

benchmarking map/Sum(2000)/scheduler/traverseConcurrently
time                 36.13 ms   (35.31 ms .. 36.92 ms)
                     0.998 R²   (0.996 R² .. 0.999 R²)
mean                 36.93 ms   (36.03 ms .. 38.43 ms)
std dev              2.456 ms   (1.045 ms .. 4.098 ms)
variance introduced by outliers: 24% (moderately inflated)

benchmarking map/Sum(2000)/unliftio/pooledTraverseConcurrently
time                 31.99 ms   (31.60 ms .. 32.39 ms)
                     0.998 R²   (0.993 R² .. 1.000 R²)
mean                 32.87 ms   (32.43 ms .. 33.90 ms)
std dev              1.454 ms   (913.4 μs .. 2.241 ms)
variance introduced by outliers: 12% (moderately inflated)

benchmarking map/Sum(2000)/streamly/mapM
time                 39.56 ms   (36.74 ms .. 43.24 ms)
                     0.988 R²   (0.977 R² .. 0.999 R²)
mean                 43.58 ms   (42.12 ms .. 44.80 ms)
std dev              2.595 ms   (1.621 ms .. 3.785 ms)
variance introduced by outliers: 20% (moderately inflated)

benchmarking map/Sum(2000)/async/mapConcurrently
time                 48.82 ms   (45.75 ms .. 51.32 ms)
                     0.992 R²   (0.983 R² .. 0.998 R²)
mean                 54.91 ms   (52.92 ms .. 57.00 ms)
std dev              3.800 ms   (2.823 ms .. 5.142 ms)
variance introduced by outliers: 22% (moderately inflated)

benchmarking map/Sum(2000)/par/mapM
time                 57.28 ms   (56.62 ms .. 58.04 ms)
                     1.000 R²   (0.999 R² .. 1.000 R²)
mean                 55.69 ms   (54.85 ms .. 56.29 ms)
std dev              1.282 ms   (994.2 μs .. 1.599 ms)

benchmarking map/Sum(2000)/monad-par/mapM
time                 237.4 ms   (198.5 ms .. 264.7 ms)
                     0.988 R²   (0.966 R² .. 1.000 R²)
mean                 257.7 ms   (242.2 ms .. 263.0 ms)
std dev              11.57 ms   (384.2 μs .. 14.53 ms)
variance introduced by outliers: 16% (moderately inflated)

benchmarking map/Sum(2000)/base/mapM
time                 147.1 ms   (141.4 ms .. 150.9 ms)
                     0.999 R²   (0.996 R² .. 1.000 R²)
mean                 152.6 ms   (150.1 ms .. 155.1 ms)
std dev              3.676 ms   (2.725 ms .. 5.066 ms)
variance introduced by outliers: 12% (moderately inflated)

```

## Beware of Demons

Any sort of concurrency primitives such as mutual exclusion, semaphores, etc. can easily lead to
deadlocks, starvation and other common problems. Try to avoid them and be careful if you do end up
using them.
