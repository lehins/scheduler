# scheduler

This is a work stealing scheduler, which is very useful for tasks parallelization.

Whenever you have many actions you'd like to perform in parallel, but would only like to use a few
threads to do the actual computation, this package is for you.

| Language | Travis | AppVeyor | Hackage | Nightly | LTS |
|:--------:|:------:|:--------:|:-------:|:-------:|:---:|
| ![GitHub top language](https://img.shields.io/github/languages/top/lehins/haskell-scheduler.svg) | [![Travis](https://img.shields.io/travis/lehins/haskell-scheduler/master.svg?label=Linux%20%26%20OS%20X)](https://travis-ci.org/lehins/haskell-scheduler) | [![AppVeyor](https://img.shields.io/appveyor/ci/lehins/haskell-scheduler/master.svg?label=Windows)](https://ci.appveyor.com/project/lehins/haskell-scheduler) | [![Hackage](https://img.shields.io/hackage/v/scheduler.svg)](https://hackage.haskell.org/package/scheduler)| [![Nightly](https://www.stackage.org/package/scheduler/badge/nightly)](https://www.stackage.org/nightly/package/scheduler) | [![Nightly](https://www.stackage.org/package/scheduler/badge/lts)](https://www.stackage.org/lts/package/scheduler) |


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

## Benchmarks

It is always good to see some benchmarks. Below is a very simple comparison of:

* [`pooledMapConcurrently`](https://hackage.haskell.org/package/unliftio-0.2.10/docs/UnliftIO-Async.html#v:pooledMapConcurrently) from `unliftio`
* [`parMapM`](http://hackage.haskell.org/package/monad-par-extras-0.3.3/docs/Control-Monad-Par-Combinator.html) from `monad-par`
* [`traverseConcurrently`](https://hackage.haskell.org/package/scheduler-1.0.0/docs/Control-Scheduler.html#v:traverseConcurrently) from `scheduler`
* [`mapConcurrently`](http://hackage.haskell.org/package/async-2.2.1/docs/Control-Concurrent-Async.html#v:mapConcurrently) from `async`
* `traverse` from `base` with
  [`par`](http://hackage.haskell.org/package/parallel-3.2.2.0/docs/Control-Parallel.html#v:par) from
  `parallel`
* Regular sequential `traverse` as a basepoint

Benchmarked function is very simple, we simply `map` a `sum` function over a list of lists. Although
`scheduler` is already doing pretty good it looks like there is still some room for improvement.


```
scheduler-1.0.1: benchmarks
Running 1 benchmarks...
Benchmark scheduler: RUNNING...
benchmarking replicate/Fib(1000/10000)/scheduler/replicateConcurrently
time                 11.25 ms   (10.01 ms .. 12.28 ms)
                     0.945 R²   (0.908 R² .. 0.975 R²)
mean                 10.82 ms   (10.18 ms .. 11.58 ms)
std dev              1.861 ms   (1.291 ms .. 2.572 ms)
variance introduced by outliers: 78% (severely inflated)

benchmarking replicate/Fib(1000/10000)/unliftio/pooledReplicateConcurrently
time                 8.941 ms   (8.576 ms .. 9.197 ms)
                     0.987 R²   (0.970 R² .. 0.995 R²)
mean                 9.070 ms   (8.764 ms .. 9.561 ms)
std dev              1.073 ms   (616.4 μs .. 1.606 ms)
variance introduced by outliers: 64% (severely inflated)

benchmarking replicate/Fib(1000/10000)/streamly/replicateM
time                 13.05 ms   (11.63 ms .. 14.31 ms)
                     0.954 R²   (0.882 R² .. 0.994 R²)
mean                 13.29 ms   (12.45 ms .. 14.80 ms)
std dev              2.845 ms   (1.269 ms .. 4.753 ms)
variance introduced by outliers: 84% (severely inflated)

benchmarking replicate/Fib(1000/10000)/async/replicateConcurrently
time                 26.88 ms   (24.23 ms .. 29.38 ms)
                     0.964 R²   (0.921 R² .. 0.988 R²)
mean                 27.38 ms   (25.96 ms .. 29.06 ms)
std dev              3.410 ms   (2.680 ms .. 4.651 ms)
variance introduced by outliers: 54% (severely inflated)

benchmarking replicate/Fib(1000/10000)/monad-par/replicateM
time                 32.28 ms   (30.29 ms .. 33.65 ms)
                     0.991 R²   (0.981 R² .. 0.998 R²)
mean                 34.14 ms   (32.75 ms .. 36.11 ms)
std dev              3.570 ms   (2.009 ms .. 5.078 ms)
variance introduced by outliers: 42% (moderately inflated)

benchmarking replicate/Fib(1000/10000)/base/replicateM
time                 45.04 ms   (41.14 ms .. 50.00 ms)
                     0.976 R²   (0.960 R² .. 0.991 R²)
mean                 37.27 ms   (34.92 ms .. 40.05 ms)
std dev              4.984 ms   (4.274 ms .. 5.931 ms)
variance introduced by outliers: 52% (severely inflated)

benchmarking replicate/Sum(1000/1000)/scheduler/replicateConcurrently
time                 17.34 ms   (16.68 ms .. 18.01 ms)
                     0.995 R²   (0.991 R² .. 0.999 R²)
mean                 19.49 ms   (18.75 ms .. 20.35 ms)
std dev              1.913 ms   (1.319 ms .. 2.557 ms)
variance introduced by outliers: 44% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/unliftio/pooledReplicateConcurrently
time                 16.79 ms   (15.90 ms .. 17.43 ms)
                     0.993 R²   (0.990 R² .. 0.997 R²)
mean                 16.12 ms   (15.87 ms .. 16.64 ms)
std dev              741.7 μs   (478.6 μs .. 1.083 ms)
variance introduced by outliers: 16% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/streamly/replicateM
time                 16.99 ms   (16.32 ms .. 17.60 ms)
                     0.990 R²   (0.984 R² .. 0.995 R²)
mean                 18.99 ms   (18.41 ms .. 19.63 ms)
std dev              1.573 ms   (1.205 ms .. 2.370 ms)
variance introduced by outliers: 39% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/async/replicateConcurrently
time                 29.23 ms   (28.43 ms .. 30.26 ms)
                     0.997 R²   (0.994 R² .. 0.999 R²)
mean                 27.57 ms   (26.85 ms .. 28.19 ms)
std dev              1.514 ms   (1.112 ms .. 1.928 ms)
variance introduced by outliers: 21% (moderately inflated)

benchmarking replicate/Sum(1000/1000)/monad-par/replicateM
time                 58.24 ms   (56.32 ms .. 61.18 ms)
                     0.997 R²   (0.994 R² .. 0.999 R²)
mean                 60.01 ms   (59.18 ms .. 60.81 ms)
std dev              1.359 ms   (961.7 μs .. 2.088 ms)

benchmarking replicate/Sum(1000/1000)/base/replicateM
time                 55.84 ms   (55.28 ms .. 56.08 ms)
                     1.000 R²   (1.000 R² .. 1.000 R²)
mean                 56.50 ms   (56.20 ms .. 57.01 ms)
std dev              675.2 μs   (420.3 μs .. 893.8 μs)

benchmarking map/Fib(2000)/scheduler/traverseConcurrently
time                 13.59 ms   (12.70 ms .. 14.58 ms)
                     0.978 R²   (0.963 R² .. 0.989 R²)
mean                 13.89 ms   (13.33 ms .. 14.66 ms)
std dev              1.594 ms   (1.131 ms .. 2.212 ms)
variance introduced by outliers: 56% (severely inflated)

benchmarking map/Fib(2000)/unliftio/pooledTraverseConcurrently
time                 7.987 ms   (7.134 ms .. 8.638 ms)
                     0.956 R²   (0.925 R² .. 0.982 R²)
mean                 7.888 ms   (7.508 ms .. 8.551 ms)
std dev              1.459 ms   (1.020 ms .. 2.173 ms)
variance introduced by outliers: 81% (severely inflated)

benchmarking map/Fib(2000)/streamly/mapM
time                 19.11 ms   (16.77 ms .. 21.11 ms)
                     0.941 R²   (0.882 R² .. 0.979 R²)
mean                 22.59 ms   (20.54 ms .. 26.62 ms)
std dev              6.343 ms   (1.834 ms .. 9.521 ms)
variance introduced by outliers: 90% (severely inflated)

benchmarking map/Fib(2000)/async/mapConcurrently
time                 34.86 ms   (30.83 ms .. 38.22 ms)
                     0.976 R²   (0.953 R² .. 0.993 R²)
mean                 43.67 ms   (40.22 ms .. 51.87 ms)
std dev              10.52 ms   (6.972 ms .. 14.64 ms)
variance introduced by outliers: 79% (severely inflated)

benchmarking map/Fib(2000)/par/mapM
time                 8.682 ms   (7.618 ms .. 9.577 ms)
                     0.942 R²   (0.924 R² .. 0.965 R²)
mean                 8.424 ms   (8.052 ms .. 9.014 ms)
std dev              1.182 ms   (982.7 μs .. 1.633 ms)
variance introduced by outliers: 71% (severely inflated)

benchmarking map/Fib(2000)/monad-par/mapM
time                 33.07 ms   (31.00 ms .. 34.73 ms)
                     0.987 R²   (0.973 R² .. 0.996 R²)
mean                 37.42 ms   (36.11 ms .. 38.95 ms)
std dev              3.231 ms   (2.093 ms .. 4.098 ms)
variance introduced by outliers: 36% (moderately inflated)

benchmarking map/Fib(2000)/base/mapM
time                 26.74 ms   (25.07 ms .. 28.32 ms)
                     0.987 R²   (0.977 R² .. 0.996 R²)
mean                 30.41 ms   (28.86 ms .. 32.48 ms)
std dev              4.134 ms   (2.797 ms .. 6.007 ms)
variance introduced by outliers: 59% (severely inflated)

benchmarking map/Sum(2000)/scheduler/traverseConcurrently
time                 41.13 ms   (38.35 ms .. 43.63 ms)
                     0.990 R²   (0.978 R² .. 0.997 R²)
mean                 39.37 ms   (38.21 ms .. 40.45 ms)
std dev              2.548 ms   (2.028 ms .. 4.232 ms)
variance introduced by outliers: 19% (moderately inflated)

benchmarking map/Sum(2000)/unliftio/pooledTraverseConcurrently
time                 30.85 ms   (29.73 ms .. 31.72 ms)
                     0.996 R²   (0.991 R² .. 0.998 R²)
mean                 34.01 ms   (33.24 ms .. 35.21 ms)
std dev              2.182 ms   (1.629 ms .. 2.792 ms)
variance introduced by outliers: 24% (moderately inflated)

benchmarking map/Sum(2000)/streamly/mapM
time                 40.59 ms   (39.14 ms .. 43.14 ms)
                     0.992 R²   (0.984 R² .. 0.997 R²)
mean                 38.08 ms   (36.46 ms .. 39.56 ms)
std dev              2.947 ms   (2.174 ms .. 3.671 ms)
variance introduced by outliers: 25% (moderately inflated)

benchmarking map/Sum(2000)/async/mapConcurrently
time                 57.13 ms   (52.84 ms .. 61.29 ms)
                     0.991 R²   (0.986 R² .. 0.998 R²)
mean                 52.29 ms   (51.04 ms .. 54.14 ms)
std dev              2.822 ms   (2.102 ms .. 3.802 ms)
variance introduced by outliers: 14% (moderately inflated)

benchmarking map/Sum(2000)/par/mapM
time                 79.09 ms   (72.89 ms .. 85.64 ms)
                     0.982 R²   (0.961 R² .. 0.997 R²)
mean                 63.74 ms   (59.19 ms .. 68.40 ms)
std dev              8.610 ms   (7.269 ms .. 10.25 ms)
variance introduced by outliers: 44% (moderately inflated)

benchmarking map/Sum(2000)/monad-par/mapM
time                 222.8 ms   (211.5 ms .. 233.7 ms)
                     0.998 R²   (0.996 R² .. 1.000 R²)
mean                 233.6 ms   (227.5 ms .. 236.6 ms)
std dev              5.712 ms   (2.215 ms .. 8.599 ms)
variance introduced by outliers: 14% (moderately inflated)

benchmarking map/Sum(2000)/base/mapM
time                 112.9 ms   (104.0 ms .. 118.4 ms)
                     0.995 R²   (0.986 R² .. 1.000 R²)
mean                 126.4 ms   (121.1 ms .. 132.3 ms)
std dev              7.872 ms   (6.986 ms .. 8.336 ms)
variance introduced by outliers: 12% (moderately inflated)
```

## Beware of Demons

Any sort of concurrency primitives such as mutual exclusion, semaphores, etc. can easily lead to
deadlocks, starvation and other common problems. Try to avoid them and be careful if you do end up
using them.
