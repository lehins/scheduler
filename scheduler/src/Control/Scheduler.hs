{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- |
-- Module      : Control.Scheduler
-- Copyright   : (c) Alexey Kuleshevich 2018-2019
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
  , waitForResults
  , waitForResults_
  , waitForResultsR
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

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Unlift
import Control.Monad.Primitive (PrimMonad)
import Control.Scheduler.Computation
import Control.Scheduler.Internal
import Control.Scheduler.Queue
import Data.Atomics (atomicModifyIORefCAS, atomicModifyIORefCAS_)
import qualified Data.Foldable as F (foldl', traverse_, toList)
import Data.IORef
import Data.Primitive.SmallArray
import Data.Primitive.MutVar
import Data.Traversable

-- | Get the underlying `Scheduler`, which cannot access `WorkerStates`.
--
-- @since 1.4.0
unwrapSchedulerWS :: SchedulerWS s m a -> Scheduler m a
unwrapSchedulerWS = _getScheduler


-- | Initialize a separate state for each worker.
--
-- @since 1.4.0
initWorkerStates :: MonadIO m => Comp -> (WorkerId -> m s) -> m (WorkerStates s)
initWorkerStates comp initState = do
  nWorkers <- getCompWorkers comp
  arr <- liftIO $ newSmallArray nWorkers (error "Uninitialized")
  let go i = when (i < nWorkers) $ do
        state <- initState (WorkerId i)
        liftIO $ writeSmallArray arr i state
        go (i + 1)
  go 0
  workerStates <- liftIO $ unsafeFreezeSmallArray arr
  mutex <- liftIO $ newIORef False
  pure
    WorkerStates
      {_workerStatesComp = comp, _workerStatesArray = workerStates, _workerStatesMutex = mutex}

-- | Get the computation strategy the states where initialized with.
--
-- @since 1.4.0
workerStatesComp :: WorkerStates s -> Comp
workerStatesComp = _workerStatesComp

withSchedulerWSInternal ::
     MonadUnliftIO m
  => (Comp -> (Scheduler m a -> t) -> m b)
  -> WorkerStates s
  -> (SchedulerWS s m a -> t)
  -> m b
withSchedulerWSInternal withScheduler' states action =
  withRunInIO $ \run -> bracket lockState unlockState (run . runSchedulerWS)
  where
    mutex = _workerStatesMutex states
    lockState = atomicModifyIORef' mutex ((,) True)
    unlockState wasLocked
      | wasLocked = pure ()
      | otherwise = writeIORef mutex False
    runSchedulerWS isLocked
      | isLocked = liftIO $ throwIO MutexException
      | otherwise =
        withScheduler' (_workerStatesComp states) $ \scheduler ->
          action (SchedulerWS states scheduler)


-- | Run a scheduler with stateful workers. Throws `MutexException` if an attempt is made
-- to concurrently use the same `WorkerStates` with another `SchedulerWS`.
--
-- ==== __Examples__
--
-- A good example of using stateful workers would be generation of random number in
-- parallel. A lof of times random number generators are not gonna be thread safe, so we
-- can work around this problem, by using a separate stateful generator for each of the
-- workers.
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
withSchedulerWS :: MonadUnliftIO m => WorkerStates s -> (SchedulerWS s m a -> m b) -> m [a]
withSchedulerWS = withSchedulerWSInternal withScheduler

-- | Run a scheduler with stateful workers, while discarding computation results.
--
-- @since 1.4.0
withSchedulerWS_ :: MonadUnliftIO m => WorkerStates s -> (SchedulerWS s m () -> m b) -> m ()
withSchedulerWS_ = withSchedulerWSInternal withScheduler_

-- | Same as `withSchedulerWS`, except instead of a list it produces `Results`, which
-- allows for distinguishing between the ways computation was terminated.
--
-- @since 1.4.2
withSchedulerWSR :: MonadUnliftIO m => WorkerStates s -> (SchedulerWS s m a -> m b) -> m (Results a)
withSchedulerWSR = withSchedulerWSInternal withSchedulerR


-- | Schedule a job that will get a worker state passed as an argument
--
-- @since 1.4.0
scheduleWorkState :: SchedulerWS s m a -> (s -> m a) -> m ()
scheduleWorkState schedulerS withState =
  scheduleWorkId (_getScheduler schedulerS) $ \(WorkerId i) ->
    withState (indexSmallArray (_workerStatesArray (_workerStates schedulerS)) i)

-- | Same as `scheduleWorkState`, but dont' keep the result of computation.
--
-- @since 1.4.0
scheduleWorkState_ :: SchedulerWS s m () -> (s -> m ()) -> m ()
scheduleWorkState_ schedulerS withState =
  scheduleWorkId_ (_getScheduler schedulerS) $ \(WorkerId i) ->
    withState (indexSmallArray (_workerStatesArray (_workerStates schedulerS)) i)


-- | Get the number of workers. Will mainly depend on the computation strategy and/or number of
-- capabilities you have. Related function is `getCompWorkers`.
--
-- @since 1.0.0
numWorkers :: Scheduler m a -> Int
numWorkers = _numWorkers


-- | Schedule an action to be picked up and computed by a worker from a pool of
-- jobs. Argument supplied to the job will be the id of the worker doing the job.
--
-- @since 1.2.0
scheduleWorkId :: Scheduler m a -> (WorkerId -> m a) -> m ()
scheduleWorkId =_scheduleWorkId

-- | As soon as possible try to terminate any computation that is being performed by all
-- workers managed by this scheduler and collect whatever results have been computed, with
-- supplied element guaranteed to being the last one. In case when `Results` is the return
-- type this function will cause the scheduler to produce `FinishedEarly`
--
-- /Important/ - With `Seq` strategy this will not stop other scheduled tasks from being computed,
-- although it will make sure their results are discarded.
--
-- @since 1.1.0
terminate :: Scheduler m a -> a -> m a
terminate = _terminate

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
terminateWith :: Scheduler m a -> a -> m a
terminateWith = _terminateWith

-- | Schedule an action to be picked up and computed by a worker from a pool of
-- jobs. Similar to `scheduleWorkId`, except the job doesn't get the worker id.
--
-- @since 1.0.0
scheduleWork :: Scheduler m a -> m a -> m ()
scheduleWork scheduler f = _scheduleWorkId scheduler (const f)


-- FIXME: get rid of scheduleJob and decide at `scheduleWork` level if we should use Job or Job_
-- Type here should be `scheduleWork_ :: Scheduler m a -> m () -> m ()
-- | Same as `scheduleWork`, but only for a `Scheduler` that doesn't keep the results.
--
-- @since 1.1.0
scheduleWork_ :: Scheduler m () -> m () -> m ()
scheduleWork_ = scheduleWork

-- | Same as `scheduleWorkId`, but only for a `Scheduler` that doesn't keep the results.
--
-- @since 1.2.0
scheduleWorkId_ :: Scheduler m () -> (WorkerId -> m ()) -> m ()
scheduleWorkId_ = _scheduleWorkId

-- | Schedule the same action to run @n@ times concurrently. This differs from
-- `replicateConcurrently` by allowing the caller to use the `Scheduler` freely,
-- or to allow early termination via `terminate` across all (identical) threads.
-- To be called within a `withScheduler` block.
--
-- @since 1.4.1
replicateWork :: Applicative m => Int -> Scheduler m a -> m a -> m ()
replicateWork !n scheduler f = go n
  where
    go !k
      | k <= 0 = pure ()
      | otherwise = scheduleWork scheduler f *> go (k - 1)

-- | Similar to `terminate`, but for a `Scheduler` that does not keep any results of computation.
--
-- /Important/ - In case of `Seq` computation strategy this function has no affect.
--
-- @since 1.1.0
terminate_ :: Scheduler m () -> m ()
terminate_ = (`_terminateWith` ())

-- | The most basic scheduler that simply runs the task instead of scheduling it. Early termination
-- requests are bluntly ignored.
--
-- @since 1.1.0
trivialScheduler_ :: Applicative f => Scheduler f ()
trivialScheduler_ = Scheduler
  { _numWorkers = 1
  , _scheduleWorkId = \f -> f (WorkerId 0)
  , _terminate = const $ pure ()
  , _terminateWith = const $ pure ()
  , _waitForResults = pure $ Finished []
  , _earlyResults = pure Nothing
  }


-- | This trivial scheduler will behave in the same way as `withScheduler` with `Seq`
-- computation strategy, except it is restricted to `PrimMonad`, instead of `MonadUnliftIO`.
--
-- @since 1.4.2
withTrivialScheduler :: PrimMonad m => (Scheduler m a -> m b) -> m [a]
withTrivialScheduler action = F.toList <$> withTrivialSchedulerR action

-- | This trivial scheduler will behave in a similar way as `withSchedulerR` with `Seq`
-- computation strategy, except it is restricted to `PrimMonad`, instead of
-- `MonadUnliftIO` and the work isn't scheduled, but rather computed immediately.
--
-- @since 1.4.2
withTrivialSchedulerR :: PrimMonad m => (Scheduler m a -> m b) -> m (Results a)
withTrivialSchedulerR action = do
  resVar <- newMutVar []
  finResVar <- newMutVar Nothing
  _ <-
    action $
    Scheduler
      { _numWorkers = 1
      , _scheduleWorkId = \f -> f (WorkerId 0) >>= \r -> r `seq` modifyMutVar' resVar (r :)
      , _terminate =
          \ !r ->
            readMutVar resVar >>= \ !rs -> r <$ writeMutVar finResVar (Just (FinishedEarly rs r))
      , _terminateWith = \ !r -> r <$ writeMutVar finResVar (Just (FinishedEarlyWith r))
      , _waitForResults =
          readMutVar finResVar >>= \case
            Just rs -> pure rs
            Nothing -> Finished <$> readMutVar resVar
      , _earlyResults = readMutVar finResVar
      }
  readMutVar finResVar >>= \case
    Just rs -> pure $ reverseResults rs
    Nothing -> Finished . Prelude.reverse <$> readMutVar resVar


withTrivialSchedulerIO_ :: MonadUnliftIO f => (Scheduler f a -> f b) -> f ()
withTrivialSchedulerIO_ action = void $ withTrivialSchedulerRIO action

withTrivialSchedulerIO :: MonadUnliftIO f => (Scheduler f a -> f b) -> f [a]
withTrivialSchedulerIO action = F.toList <$> withTrivialSchedulerRIO action

withTrivialSchedulerRIO :: MonadUnliftIO m => (Scheduler m a -> m b) -> m (Results a)
withTrivialSchedulerRIO action = do
  resVar <- liftIO $ newIORef []
  finResVar <- liftIO $ newIORef Nothing
  _ <-
    action $
    Scheduler
      { _numWorkers = 1
      , _scheduleWorkId =
          \f -> do
            r <- f (WorkerId 0)
            r `seq` liftIO (atomicModifyIORefCAS_ resVar (r :))
      , _terminate =
          \ !r ->
            liftIO $ do
              rs <- readIORef resVar
              r <$ writeIORef finResVar (Just (FinishedEarly rs r))
      , _terminateWith = \ !r -> r <$ liftIO (writeIORef finResVar (Just (FinishedEarlyWith r)))
      , _waitForResults =
          liftIO (readIORef finResVar) >>= \case
            Just rs -> pure rs
            Nothing -> Finished <$> liftIO (readIORef resVar)
      , _earlyResults = liftIO (readIORef finResVar)
      }
  liftIO (readIORef finResVar) >>= \case
    Just rs -> pure $ reverseResults rs
    Nothing -> Finished . Prelude.reverse <$> liftIO (readIORef resVar)


-- | This is generally a faster way to traverse while ignoring the result rather than using `mapM_`.
--
-- @since 1.0.0
traverse_ :: (Applicative f, Foldable t) => (a -> f ()) -> t a -> f ()
traverse_ f = F.foldl' (\c a -> c *> f a) (pure ())

-- | Map an action over each element of the `Traversable` @t@ acccording to the supplied computation
-- strategy.
--
-- @since 1.0.0
traverseConcurrently :: (MonadUnliftIO m, Traversable t) => Comp -> (a -> m b) -> t a -> m (t b)
traverseConcurrently comp f xs = do
  ys <- withScheduler comp $ \s -> traverse_ (scheduleWork s . f) xs
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
  withScheduler_ comp $ \s -> scheduleWork s $ F.traverse_ (scheduleWork s . void . f) xs

-- | Replicate an action @n@ times and schedule them acccording to the supplied computation
-- strategy.
--
-- @since 1.1.0
replicateConcurrently :: MonadUnliftIO m => Comp -> Int -> m a -> m [a]
replicateConcurrently comp n f =
  withScheduler comp $ \s -> replicateM_ n $ scheduleWork s f

-- | Just like `replicateConcurrently`, but discards the results of computation.
--
-- @since 1.1.0
replicateConcurrently_ :: MonadUnliftIO m => Comp -> Int -> m a -> m ()
replicateConcurrently_ comp n f =
  withScheduler_ comp $ \s -> scheduleWork s $ replicateM_ n (scheduleWork s $ void f)


scheduleJobs :: MonadIO m => Jobs m a -> (WorkerId -> m a) -> m ()
scheduleJobs = scheduleJobsWith mkJob

-- | Ignores the result of computation, thus having less overhead.
scheduleJobs_ :: MonadIO m => Jobs m a -> (WorkerId -> m b) -> m ()
scheduleJobs_ = scheduleJobsWith (\job -> pure (Job_ (void . job (\_ -> pure ()))))

scheduleJobsWith ::
     MonadIO m
  => (((b -> m ()) -> WorkerId -> m b) -> m (Job m a))
  -> Jobs m a
  -> (WorkerId -> m b)
  -> m ()
scheduleJobsWith mkJob' Jobs {..} action = do
  liftIO $ do
    prevCount <- atomicModifyIORefCAS jobsCountRef (\prev -> (prev + 1, prev))
    when (prevCount == 0) $ void $ takeMVar jobsSchedulerStatus
  job <-
    mkJob' $ \storeResult i -> do
      res <- action i
      res `seq` storeResult res
      dropCounterOnZero jobsCountRef $ liftIO $ void $ tryPutMVar jobsSchedulerStatus SchedulerIdle
      return res
  pushJQueue jobsQueue job


-- | Decrease a counter by one and perform an action when it drops down to zero.
dropCounterOnZero :: MonadIO m => IORef Int -> m () -> m ()
dropCounterOnZero counterRef onZero = do
  jc <-
    liftIO $
    atomicModifyIORefCAS
      counterRef
      (\ !i' ->
         let !i = i' - 1
          in (i, i))
  when (jc == 0) onZero


-- | Runs the worker until it is terminated with an `WorkerTerminateException` or killed
-- by some other asynchronous exception.
runWorker :: MonadUnliftIO m => WorkerId -> Jobs m a -> m ()
runWorker wId Jobs {jobsQueue, jobsSchedulerStatus} = go
  where
    go = do
      job <- popJQueue jobsQueue
      withRunInIO $ \run ->
        catch (run (job wId)) $ \exc -> do
          unless (isSyncException exc) $ throwIO exc
          void $ tryPutMVar jobsSchedulerStatus (SchedulerWorkerException (WorkerException exc))
      go


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
  -> (Scheduler m a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m [a]
withScheduler Seq = withTrivialSchedulerIO
withScheduler comp = withSchedulerInternal comp scheduleJobs readResults (reverse . resultsToList)

-- | Same as `withScheduler`, except instead of a list it produces `Results`, which allows
-- for distinguishing between the ways computation was terminated.
--
-- @since 1.4.2
withSchedulerR ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Scheduler m a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m (Results a)
withSchedulerR Seq = withTrivialSchedulerRIO
withSchedulerR comp = withSchedulerInternal comp scheduleJobs readResults reverseResults


-- | Same as `withScheduler`, but discards results of submitted jobs.
--
-- @since 1.0.0
withScheduler_ ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Scheduler m a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m ()
withScheduler_ Seq = withTrivialSchedulerIO_
withScheduler_ comp = void . withSchedulerInternal comp scheduleJobs_ (const (pure [])) (const ())


-- | Wait for all scheduled jobs to be done and collect the computed results into a
-- list. It is a blocking operation, but if there are no jobs in progress it will return
-- immediately. It is safe to continue using the supplied scheduler after this function
-- returns, but if any of the jobs resulted in an exception it will be rethrown by this
-- function, which will also put the scheduler in a terminated state.
--
-- @since 1.4.3
waitForResults :: Functor m => Scheduler m a -> m [a]
waitForResults Scheduler {_waitForResults} = reverse . resultsToList <$> _waitForResults

-- | Same as `waitForResults` but discard the results
--
-- @since 1.4.3
waitForResults_ :: Monad m => Scheduler m a -> m ()
waitForResults_ Scheduler {_waitForResults} = void _waitForResults

-- | Same as `waitForResults`, but returns the actual `Results` data type.
--
-- @since 1.4.3
waitForResultsR :: Functor m => Scheduler m a -> m (Results a)
waitForResultsR Scheduler {_waitForResults} = reverseResults <$> _waitForResults



initScheduler ::
     MonadIO m
  => Comp
  -> (Jobs m a -> (WorkerId -> m a) -> m ())
  -> (JQueue m a -> m [a])
  -> m (Jobs m a, Scheduler m a)
initScheduler comp submitWork collect = do
  jobsNumWorkers <- getCompWorkers comp
  jobsQueue <- newJQueue
  jobsCountRef <- liftIO $ newIORef 0
  jobsSchedulerStatus <- liftIO $ newMVar SchedulerIdle
  earlyTerminationResultRef <- liftIO $ newIORef Nothing
  let jobs =
        Jobs
          { jobsNumWorkers = jobsNumWorkers
          , jobsQueue = jobsQueue
          , jobsCountRef = jobsCountRef
          , jobsSchedulerStatus = jobsSchedulerStatus
          }
      blockForResults =
        liftIO (readMVar jobsSchedulerStatus) >>= \case
          SchedulerWorkerException (WorkerException exc) -> liftIO $ throwIO exc
          SchedulerIdle -> Finished <$> collect jobsQueue
      scheduler =
        Scheduler
          { _numWorkers = jobsNumWorkers
          , _scheduleWorkId = submitWork jobs
          , _terminate =
              \a -> do
                as <- collect jobsQueue
                liftIO $ do
                  writeIORef earlyTerminationResultRef $ Just $ FinishedEarly as a
                  throwIO TerminateEarlyException
          , _terminateWith =
              \a ->
                liftIO $ do
                  writeIORef earlyTerminationResultRef $ Just $ FinishedEarlyWith a
                  throwIO TerminateEarlyException
          , _waitForResults = blockForResults
          , _earlyResults = liftIO (readIORef earlyTerminationResultRef)
          }
  pure (jobs, scheduler)


withSchedulerInternal ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Jobs m a -> (WorkerId -> m a) -> m ()) -- ^ How to schedule work
  -> (JQueue m a -> m [a]) -- ^ How to collect results
  -> (Results a -> c) -- ^ Adjust results in some way
  -> (Scheduler m a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m c
withSchedulerInternal comp submitWork collect adjust onScheduler = do
  (jobs@Jobs{..}, scheduler) <- initScheduler comp submitWork collect
  -- / Wait for the initial jobs to get scheduled before spawining off the workers, otherwise it
  -- would be trickier to identify the beginning and the end of a job pool.
  let spawnWorkersWith fork ws =
        withRunInIO $ \run ->
          forM (zip [0 ..] ws) $ \(wId, on) ->
            fork on $ \unmask ->
              catch
                (unmask $ run $ runWorker wId jobs)
                (\exc -> run $ handleWorkerException jobs exc)
      spawnWorkers =
        case comp of
          Par -> spawnWorkersWith forkOnWithUnmask [1 .. jobsNumWorkers]
          ParOn ws -> spawnWorkersWith forkOnWithUnmask ws
          ParN _ -> spawnWorkersWith (\_ -> forkIOWithUnmask) [1 .. jobsNumWorkers]
          Seq -> spawnWorkersWith (\_ -> forkIOWithUnmask) [1 :: Int]
            -- \ sequential computation is suboptimal when used in this way.
      terminateWorkers = liftIO . traverse_ (`throwTo` SomeAsyncException WorkerTerminateException)
      readEarlyTermination =
        _earlyResults scheduler >>= \case
          Nothing -> error "Impossible: uninitialized early termination value"
          Just rs -> pure $ adjust rs
      doWork tids =
        withRunInIO $ \run ->
          try (run (onScheduler scheduler)) >>= \case
            Left TerminateEarlyException ->
              run $ do
                terminateWorkers tids
                readEarlyTermination
            Right _ -> do
              schedulerStatus <- liftIO (takeMVar jobsSchedulerStatus)
              -- \ wait for all worker to finish. If any one of the workers had a problem, then
              -- this MVar will contain an exception
              case schedulerStatus of
                SchedulerWorkerException (WorkerException exc) -> do
                  run $ terminateWorkers tids
                  case fromException exc of
                    Just TerminateEarlyException -> run readEarlyTermination
                    Nothing -> throwIO exc
                -- \ Here we need to unwrap the legit worker exception and rethrow it, so
                -- the main thread will think like it's his own
                SchedulerIdle ->
                  run $ do
                    terminateWorkers tids
                    adjust . Finished <$> collect jobsQueue
                -- \ Now we are sure all workers have done their job we can safely read
                -- all of the IORefs with results
  safeBracketOnError spawnWorkers terminateWorkers doWork

resultsToList :: Results a -> [a]
resultsToList = \case
  Finished rs -> rs
  FinishedEarly rs r -> r:rs
  FinishedEarlyWith r -> [r]


reverseResults :: Results a -> Results a
reverseResults = \case
  Finished rs -> Finished (reverse rs)
  FinishedEarly rs r -> FinishedEarly (reverse rs) r
  res -> res


-- | Specialized exception handler for the work scheduler.
handleWorkerException :: MonadIO m => Jobs m a -> SomeException -> m ()
handleWorkerException Jobs {jobsSchedulerStatus} exc = do
  case asyncExceptionFromException exc of
    Just WorkerTerminateException -> return ()
      -- \ in a process of worker termination
      --  --------------------------------------------------------
    _ ->
      liftIO $ do
        void $ tryPutMVar jobsSchedulerStatus $ SchedulerWorkerException $ WorkerException exc
        unless (isSyncException exc) $ throwIO exc
        -- \ Main thread must know how we died and if it was async exception, rethrow it.


-- Copies from unliftio

safeBracketOnError :: MonadUnliftIO m => m a -> (a -> m b) -> (a -> m c) -> m c
safeBracketOnError before after thing =
  withRunInIO $ \run ->
    mask $ \restore -> do
      x <- run before
      res1 <- try $ restore $ run $ thing x
      case res1 of
        Left (e1 :: SomeException) -> do
          _ :: Either SomeException b <- try $ uninterruptibleMask_ $ run $ after x
          throwIO e1
        Right y -> return y

isSyncException :: Exception e => e -> Bool
isSyncException exc =
  case fromException (toException exc) of
    Just (SomeAsyncException _) -> False
    Nothing -> True


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
