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
benchmarking Sums: 1000/unliftio/pooledMapConcurrently
time                 57.60 ms   (56.28 ms .. 58.42 ms)
                     0.999 R²   (0.997 R² .. 1.000 R²)
mean                 56.40 ms   (54.90 ms .. 57.89 ms)
std dev              2.696 ms   (1.725 ms .. 4.221 ms)
variance introduced by outliers: 15% (moderately inflated)

benchmarking Sums: 1000/monad-par/parMapM
time                 58.27 ms   (57.22 ms .. 59.13 ms)
                     0.999 R²   (0.998 R² .. 1.000 R²)
mean                 59.68 ms   (59.05 ms .. 60.94 ms)
std dev              1.814 ms   (837.5 μs .. 2.646 ms)

benchmarking Sums: 1000/scheduler/traverseConcurrently
time                 60.76 ms   (59.81 ms .. 61.37 ms)
                     1.000 R²   (0.999 R² .. 1.000 R²)
mean                 60.69 ms   (60.16 ms .. 61.91 ms)
std dev              1.255 ms   (421.4 μs .. 2.129 ms)

benchmarking Sums: 1000/async/mapConcurrently
time                 96.35 ms   (94.71 ms .. 97.40 ms)
                     1.000 R²   (0.999 R² .. 1.000 R²)
mean                 96.59 ms   (95.40 ms .. 97.72 ms)
std dev              1.923 ms   (1.215 ms .. 2.844 ms)

benchmarking Sums: 1000/base/traverse . par
time                 86.63 ms   (77.73 ms .. 96.38 ms)
                     0.981 R²   (0.967 R² .. 0.999 R²)
mean                 78.41 ms   (75.96 ms .. 82.60 ms)
std dev              5.374 ms   (2.328 ms .. 8.587 ms)
variance introduced by outliers: 19% (moderately inflated)

benchmarking Sums: 1000/base/traverse . seq
time                 387.5 ms   (325.4 ms .. 451.6 ms)
                     0.996 R²   (0.987 R² .. 1.000 R²)
mean                 445.7 ms   (416.9 ms .. 473.8 ms)
std dev              32.93 ms   (27.12 ms .. 37.56 ms)
variance introduced by outliers: 21% (moderately inflated)
```

## Beware of Demons

Any sort of concurrency primitives such as mutual exclusion, semaphores, etc. can easily lead to
deadlocks, starvation and other common problems. Try to avoid them and be careful if you do end up
using them.

