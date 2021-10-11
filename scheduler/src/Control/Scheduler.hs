{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}
-- |
-- Module      : Control.Scheduler
-- Copyright   : (c) Alexey Kuleshevich 2018-2021
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler
  ( -- * Scheduler
    Scheduler
  , SchedulerWS
  , Results(..)
    -- ** Regular
  , withScheduler
  , withScheduler_
  , withSchedulerR
    -- ** Stateful workers
  , withSchedulerWS
  , withSchedulerWS_
  , withSchedulerWSR
  , unwrapSchedulerWS
    -- ** Trivial (no parallelism)
  , trivialScheduler_
  , withTrivialScheduler
  , withTrivialSchedulerR
  -- * Scheduling computation
  , scheduleWork
  , scheduleWork_
  , scheduleWorkId
  , scheduleWorkId_
  , scheduleWorkState
  , scheduleWorkState_
  , replicateWork
  , replicateWork_
  -- * Batches
  , Batch
  , runBatch
  , runBatch_
  , runBatchR
  , cancelBatch
  , cancelBatch_
  , cancelBatchWith
  , hasBatchFinished
  , getCurrentBatch
  -- * Early termination
  , terminate
  , terminate_
  , terminateWith
  -- * Workers
  , WorkerId(..)
  , WorkerStates
  , numWorkers
  , workerStatesComp
  , initWorkerStates
  -- * Computation strategies
  , Comp(..)
  , getCompWorkers
  -- * Useful functions
  , replicateConcurrently
  , replicateConcurrently_
  , traverseConcurrently
  , traverseConcurrently_
  , traverse_
  -- * Exceptions
  -- $exceptions
  , MutexException(..)
  ) where

import Control.Monad
import Control.Monad.ST
import Control.Monad.IO.Unlift
import Control.Monad.Primitive
import Control.Scheduler.Computation
import Control.Scheduler.Internal
import Control.Scheduler.Types
import Control.Scheduler.Queue
import qualified Data.Foldable as F (traverse_, toList)
import Data.Primitive.SmallArray
import Data.Traversable


-- | Get the underlying `Scheduler`, which cannot access `WorkerStates`.
--
-- @since 1.4.0
unwrapSchedulerWS :: SchedulerWS ws a -> Scheduler RealWorld a
unwrapSchedulerWS = _getScheduler


-- | Get the computation strategy the states where initialized with.
--
-- @since 1.4.0
workerStatesComp :: WorkerStates ws -> Comp
workerStatesComp = _workerStatesComp


-- | Run a scheduler with stateful workers. Throws `MutexException` if an attempt is made
-- to concurrently use the same `WorkerStates` with another `SchedulerWS`.
--
-- ==== __Examples__
--
-- A good example of using stateful workers would be generation of random number
-- in parallel. A lof of times random number generators are not thread safe, so
-- we can work around this problem with a separate stateful generator for
-- each of the workers.
--
-- >>> import Control.Monad as M ((>=>), replicateM)
-- >>> import Control.Concurrent (yield, threadDelay)
-- >>> import Data.List (sort)
-- >>> -- ^ Above imports are used to make sure output is deterministic, which is needed for doctest
-- >>> import System.Random.MWC as MWC
-- >>> import Data.Vector.Unboxed as V (singleton)
-- >>> states <- initWorkerStates (ParN 4) (MWC.initialize . V.singleton . fromIntegral . getWorkerId)
-- >>> let scheduleGen scheduler = scheduleWorkState scheduler (MWC.uniform >=> \r -> yield >> threadDelay 200000 >> pure r)
-- >>> sort <$> withSchedulerWS states (M.replicateM 4 . scheduleGen) :: IO [Double]
-- [0.21734983682025255,0.5000843862105709,0.5759825622603018,0.8587171114177893]
-- >>> sort <$> withSchedulerWS states (M.replicateM 4 . scheduleGen) :: IO [Double]
-- [2.3598617298033475e-2,9.949679290089553e-2,0.38223134248645885,0.7408640677124702]
--
-- In the above example we use four different random number generators from
-- [`mwc-random`](https://www.stackage.org/package/mwc-random) in order to generate 4
-- numbers, all in separate threads. The subsequent call to the `withSchedulerWS` function
-- with the same @states@ is allowed to reuse the same generators, thus avoiding expensive
-- initialization.
--
-- /Side note/ - The example presented was crafted with slight trickery in order to
-- guarantee that the output is deterministic, so if you run instructions exactly the same
-- way in GHCI you will get the exact same output. Non-determinism comes from thread
-- scheduling, rather than from random number generator, because we use exactly the same
-- seed for each worker, but workers run concurrently. Exact output is not really needed,
-- except for the doctests to pass.
--
-- @since 1.4.0
withSchedulerWS ::
     MonadUnliftIO m => WorkerStates ws -> (SchedulerWS ws a -> m b) -> m [a]
withSchedulerWS = withSchedulerWSInternal withScheduler

-- | Run a scheduler with stateful workers, while discarding computation results.
--
-- @since 1.4.0
withSchedulerWS_ ::
     MonadUnliftIO m => WorkerStates ws -> (SchedulerWS ws () -> m b) -> m ()
withSchedulerWS_ = withSchedulerWSInternal withScheduler_

-- | Same as `withSchedulerWS`, except instead of a list it produces `Results`, which
-- allows for distinguishing between the ways computation was terminated.
--
-- @since 1.4.2
withSchedulerWSR ::
     MonadUnliftIO m
  => WorkerStates ws
  -> (SchedulerWS ws a -> m b)
  -> m (Results a)
withSchedulerWSR = withSchedulerWSInternal withSchedulerR


-- | Schedule a job that will get a worker state passed as an argument
--
-- @since 1.4.0
scheduleWorkState :: MonadPrimBase RealWorld m => SchedulerWS ws a -> (ws -> m a) -> m ()
scheduleWorkState schedulerS withState =
  scheduleWorkId (_getScheduler schedulerS) $ \(WorkerId i) ->
    withState (indexSmallArray (_workerStatesArray (_workerStates schedulerS)) i)

-- | Same as `scheduleWorkState`, but dont' keep the result of computation.
--
-- @since 1.4.0
scheduleWorkState_ :: MonadPrimBase RealWorld m => SchedulerWS ws () -> (ws -> m ()) -> m ()
scheduleWorkState_ schedulerS withState =
  scheduleWorkId_ (_getScheduler schedulerS) $ \(WorkerId i) ->
    withState (indexSmallArray (_workerStatesArray (_workerStates schedulerS)) i)


-- | Get the number of workers. Will mainly depend on the computation strategy and/or number of
-- capabilities you have. Related function is `getCompWorkers`.
--
-- @since 1.0.0
numWorkers :: Scheduler s a -> Int
numWorkers = _numWorkers


-- | Schedule an action to be picked up and computed by a worker from a pool of
-- jobs. Argument supplied to the job will be the id of the worker doing the job. This is
-- useful for identification of a thread that will be doing the work, since there is
-- one-to-one mapping from `Control.Concurrent.ThreadId` to `WorkerId` for a particular
-- scheduler.
--
-- @since 1.2.0
scheduleWorkId :: MonadPrimBase s m => Scheduler s a -> (WorkerId -> m a) -> m ()
scheduleWorkId s f = stToPrim (_scheduleWorkId s (primToPrim . f))

-- | As soon as possible try to terminate any computation that is being performed by all
-- workers managed by this scheduler and collect whatever results have been computed, with
-- supplied element guaranteed to being the last one. In case when `Results` type is
-- returned this function will cause the scheduler to produce `FinishedEarly`
--
-- /Important/ - With `Seq` strategy this will not stop other scheduled tasks from being computed,
-- although it will make sure their results are discarded.
--
-- @since 1.1.0
terminate :: MonadPrim s m => Scheduler s a -> a -> m a
terminate scheduler a = stToPrim (_terminate scheduler (Early a))

-- | Same as `terminate`, but returning a single element list containing the supplied
-- argument. This can be very useful for parallel search algorithms. In case when
-- `Results` is the return type this function will cause the scheduler to produce
-- `FinishedEarlyWith`
--
-- /Important/ - Same as with `terminate`, when `Seq` strategy is used, this will not prevent
-- computation from continuing, but the scheduler will return only the result supplied to this
-- function.
--
-- @since 1.1.0
terminateWith :: MonadPrim s m => Scheduler s a -> a -> m a
terminateWith scheduler a = stToPrim $ _terminate scheduler (EarlyWith a)

-- | Schedule an action to be picked up and computed by a worker from a pool of
-- jobs. Similar to `scheduleWorkId`, except the job doesn't get the worker id.
--
-- @since 1.0.0
scheduleWork :: MonadPrimBase s m => Scheduler s a -> m a -> m ()
scheduleWork scheduler f = stToPrim $ _scheduleWorkId scheduler (const (primToPrim f))


-- FIXME: get rid of scheduleJob and decide at `scheduleWork` level if we should use Job or Job_
-- Type here should be `scheduleWork_ :: Scheduler s a -> m () -> m ()
-- | Same as `scheduleWork`, but only for a `Scheduler` that doesn't keep the results.
--
-- @since 1.1.0
scheduleWork_ :: MonadPrimBase s m => Scheduler s () -> m () -> m ()
scheduleWork_ s = stToPrim . scheduleWork s . primToPrim

-- | Same as `scheduleWorkId`, but only for a `Scheduler` that doesn't keep the results.
--
-- @since 1.2.0
scheduleWorkId_ :: MonadPrimBase s m => Scheduler s () -> (WorkerId -> m ()) -> m ()
scheduleWorkId_ scheduler f = stToPrim $ _scheduleWorkId scheduler (primToPrim . f)

-- | Schedule the same action to run @n@ times concurrently. This differs from
-- `replicateConcurrently` by allowing the caller to use the `Scheduler` freely,
-- or to allow early termination via `terminate` across all (identical) threads.
-- To be called within a `withScheduler` block.
--
-- @since 2.0.0
replicateWork :: MonadPrimBase s m => Scheduler s a -> Int -> m a -> m ()
replicateWork scheduler n f = go n
  where
    go !k
      | k <= 0 = pure ()
      | otherwise = stToPrim (scheduleWork scheduler (primToPrim f)) *> go (k - 1)


-- | Same as `replicateWork`, but it does not retain the results of scheduled jobs
--
-- @since 2.0.0
replicateWork_ :: MonadPrimBase s m => Scheduler s () -> Int -> m a -> m ()
replicateWork_ scheduler n f = go n
  where
    go !k
      | k <= 0 = pure ()
      | otherwise = stToPrim (scheduleWork_ scheduler (primToPrim (void f))) *> go (k - 1)

-- | Similar to `terminate`, but for a `Scheduler` that does not keep any results of computation.
--
-- /Important/ - In case of `Seq` computation strategy this function has no affect.
--
-- @since 1.1.0
terminate_ :: MonadPrim s m => Scheduler s () -> m ()
terminate_ s = stToPrim $ _terminate s (Early ())


-- | This trivial scheduler will behave in the same way as `withScheduler` with `Seq`
-- computation strategy, except it is restricted to `PrimMonad`, instead of `MonadUnliftIO`.
--
-- @since 1.4.2
withTrivialScheduler :: MonadPrim s m => (Scheduler s a -> m b) -> m [a]
withTrivialScheduler action = F.toList <$> withTrivialSchedulerR action



-- | Map an action over each element of the `Traversable` @t@ acccording to the supplied computation
-- strategy.
--
-- @since 1.0.0
traverseConcurrently :: (MonadUnliftIO m, Traversable t) => Comp -> (a -> m b) -> t a -> m (t b)
traverseConcurrently comp f xs = withRunInIO $ \run -> do
  ys <- withScheduler comp $ \s -> traverse_ (scheduleWork s . run . f) xs
  pure $ transList ys xs

transList :: Traversable t => [a] -> t b -> t a
transList xs' = snd . mapAccumL withR xs'
  where
    withR (x:xs) _ = (xs, x)
    withR _      _ = errorWithoutStackTrace "Impossible<traverseConcurrently> - Mismatched sizes"

-- | Just like `traverseConcurrently`, but restricted to `Foldable` and discards the results of
-- computation.
--
-- @since 1.0.0
traverseConcurrently_ :: (MonadUnliftIO m, Foldable t) => Comp -> (a -> m b) -> t a -> m ()
traverseConcurrently_ comp f xs =
  withRunInIO $ \run ->
    withScheduler_ comp $ \s -> scheduleWork s $ F.traverse_ (scheduleWork s . void . run . f) xs

-- | Replicate an action @n@ times and schedule them acccording to the supplied computation
-- strategy.
--
-- @since 1.1.0
replicateConcurrently :: MonadUnliftIO m => Comp -> Int -> m a -> m [a]
replicateConcurrently comp n f =
  withRunInIO $ \run ->
    withScheduler comp $ \s -> replicateM_ n $ scheduleWork s (run f)

-- | Just like `replicateConcurrently`, but discards the results of computation.
--
-- @since 1.1.0
replicateConcurrently_ :: MonadUnliftIO m => Comp -> Int -> m a -> m ()
replicateConcurrently_ comp n f =
  withRunInIO $ \run -> do
    withScheduler_ comp $ \s -> scheduleWork s $ replicateM_ n (scheduleWork s $ void $ run f)




-- | Initialize a scheduler and submit jobs that will be computed sequentially or in parallelel,
-- which is determined by the `Comp`utation strategy.
--
-- Here are some cool properties about the `withScheduler`:
--
-- * This function will block until all of the submitted jobs have finished or at least one of them
--   resulted in an exception, which will be re-thrown at the callsite.
--
-- * It is totally fine for nested jobs to submit more jobs for the same or other scheduler
--
-- * It is ok to initialize multiple schedulers at the same time, although that will likely result
--   in suboptimal performance, unless workers are pinned to different capabilities.
--
-- * __Warning__ It is pretty dangerous to schedule jobs that can block, because it might
--   lead to a potential deadlock, if you are not careful. Consider this example. First
--   execution works fine, since there are two scheduled workers, and one can unblock the
--   other, but the second scenario immediately results in a deadlock.
--
-- >>> withScheduler (ParOn [1,2]) $ \s -> newEmptyMVar >>= (\ mv -> scheduleWork s (readMVar mv) >> scheduleWork s (putMVar mv ()))
-- [(),()]
-- >>> import System.Timeout
-- >>> timeout 1000000 $ withScheduler (ParOn [1]) $ \s -> newEmptyMVar >>= (\ mv -> scheduleWork s (readMVar mv) >> scheduleWork s (putMVar mv ()))
-- Nothing
--
-- __Important__: In order to get work done truly in parallel, program needs to be compiled with
-- @-threaded@ GHC flag and executed with @+RTS -N -RTS@ to use all available cores.
--
-- @since 1.0.0
withScheduler ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Scheduler RealWorld a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m [a]
withScheduler Seq f =
  withRunInIO $ \run -> do
    reverse . resultsToList <$> withTrivialSchedulerRIO (run . f)
withScheduler comp f =
  withRunInIO $ \run -> do
    reverse . resultsToList <$> withSchedulerInternal comp scheduleJobs readResults (run . f)
{-# INLINE withScheduler #-}

-- | Same as `withScheduler`, except instead of a list it produces `Results`, which allows
-- for distinguishing between the ways computation was terminated.
--
-- @since 1.4.2
withSchedulerR ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Scheduler RealWorld a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m (Results a)
withSchedulerR Seq f =
  withRunInIO $ \run -> do
    reverseResults <$> withTrivialSchedulerRIO (run . f)
withSchedulerR comp f =
  withRunInIO $ \run -> do
    reverseResults <$> withSchedulerInternal comp scheduleJobs readResults (run .f)
{-# INLINE withSchedulerR #-}


-- | Same as `withScheduler`, but discards results of submitted jobs.
--
-- @since 1.0.0
withScheduler_ ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Scheduler RealWorld a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m ()
withScheduler_ Seq f =
  withRunInIO $ \run -> do
    void $ withTrivialSchedulerRIO (run . f)
withScheduler_ comp f =
  withRunInIO $ \run -> do
    void $ withSchedulerInternal comp scheduleJobs_ (const (pure [])) (run . f)
{-# INLINE withScheduler_ #-}



-- | Check if the supplied batch has already finished.
--
-- @since 1.5.0
hasBatchFinished :: MonadPrim s m => Batch s a -> m Bool
hasBatchFinished = stToPrim . batchHasFinished
{-# INLINE hasBatchFinished #-}


-- | Cancel batch with supplied identifier, which will lead to scheduler to return
-- `FinishedEarly` result. This is an idempotent operation and has no affect if currently
-- running batch does not match supplied identifier. Returns `False` when cancelling did
-- not succeed due to mismatched identifier or does not return at all since all jobs get
-- cancelled immediately. For trivial schedulers however there is no way to perform
-- concurrent cancelation and it will return `True`.
--
-- @since 1.5.0
cancelBatch :: MonadPrim s m => Batch s a -> a -> m Bool
cancelBatch b = stToPrim . batchCancel b
{-# INLINE cancelBatch #-}

-- | Same as `cancelBatch`, but only works with schedulers that don't care about results
--
-- @since 1.5.0
cancelBatch_ :: MonadPrim s m => Batch s () -> m Bool
cancelBatch_ b = stToPrim $ batchCancel b ()
{-# INLINE cancelBatch_ #-}

-- | Same as `cancelBatch_`, but the result of computation will be set to `FinishedEarlyWith`
--
-- @since 1.5.0
cancelBatchWith :: MonadPrim s m => Batch s a -> a -> m Bool
cancelBatchWith b = stToPrim . batchCancelWith b
{-# INLINE cancelBatchWith #-}


-- | This function gives a way to get access to the main batch that started implicitely.
--
-- @since 1.5.0
getCurrentBatch ::
     MonadPrim s m => Scheduler s a -> m (Batch s a)
getCurrentBatch scheduler = stToPrim $ do
  batchId <- _currentBatchId scheduler
  pure $ Batch
    { batchCancel = _cancelBatch scheduler batchId . Early
    , batchCancelWith = _cancelBatch scheduler batchId . EarlyWith
    , batchHasFinished = (batchId /=) <$> _currentBatchId scheduler
    }
{-# INLINE getCurrentBatch #-}


-- | Run a single batch of jobs. Supplied action will not return until all jobs placed on
-- the queue are done or the whole batch is cancelled with one of these `cancelBatch`,
-- `cancelBatch_` or `cancelBatchWith`.
--
-- It waits for all scheduled jobs to finish and collects the computed results into a
-- list. It is a blocking operation, but if there are no jobs in progress it will return
-- immediately. It is safe to continue using the supplied scheduler after this function
-- returns. However, if any of the jobs resulted in an exception it will be rethrown by this
-- function, which, unless caught, will further put the scheduler in a terminated state.
--
-- It is important to note that any job that hasn't had its results collected from the
-- scheduler prior to starting the batch it will end up on the batch result list.
--
-- @since 1.5.0
runBatch :: MonadPrimBase s m => Scheduler s a -> (Batch s a -> m c) -> m [a]
runBatch scheduler f = stToPrim $ do
  _ <- primToPrim . f =<< getCurrentBatch scheduler
  reverse . resultsToList <$> _waitForCurrentBatch scheduler
{-# INLINE runBatch #-}

-- | Same as `runBatch`, except it ignores results of computation
--
-- @since 1.5.0
runBatch_ ::
     MonadPrimBase s m => Scheduler s () -> (Batch s () -> m c) -> m ()
runBatch_ scheduler f = stToPrim $ do
  _ <- primToPrim . f =<< getCurrentBatch scheduler
  void (_waitForCurrentBatch scheduler)
{-# INLINE runBatch_ #-}


-- | Same as `runBatch`, except it produces `Results` instead of a list.
--
-- @since 1.5.0
runBatchR ::
     MonadPrimBase s m => Scheduler s a -> (Batch s a -> m c) -> m (Results a)
runBatchR scheduler f = stToPrim $ do
  _ <- primToPrim . f =<< getCurrentBatch scheduler
  reverseResults <$> _waitForCurrentBatch scheduler
{-# INLINE runBatchR #-}

{- $setup

>>> import Control.Exception
>>> import Control.Concurrent
>>> import Control.Concurrent.MVar

-}


{- $exceptions

If any one of the workers dies with an exception, even if that exceptions is asynchronous, it will be
re-thrown in the scheduling thread.


>>> let didAWorkerDie = handleJust asyncExceptionFromException (return . (== ThreadKilled)) . fmap or
>>> :t didAWorkerDie
didAWorkerDie :: Foldable t => IO (t Bool) -> IO Bool
>>> didAWorkerDie $ withScheduler Par $ \ s -> scheduleWork s $ pure False
False
>>> didAWorkerDie $ withScheduler Par $ \ s -> scheduleWork s $ myThreadId >>= killThread >> pure False
True
>>> withScheduler Par $ \ s -> scheduleWork s $ myThreadId >>= killThread >> pure False
*** Exception: thread killed

-}
