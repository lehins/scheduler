{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Unsafe #-}
{-# OPTIONS_HADDOCK hide, not-home #-}
-- |
-- Module      : Control.Scheduler.Internal
-- Copyright   : (c) Alexey Kuleshevich 2018-2020
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Internal
  ( withSchedulerInternal
  , initWorkerStates
  , withSchedulerWSInternal
  , trivialScheduler_
  , withTrivialSchedulerR
  , withTrivialSchedulerRIO
  , initScheduler
  , spawnWorkers
  , terminateWorkers
  , scheduleJobs
  , scheduleJobs_
  , scheduleJobsWith
  , reverseResults
  , resultsToList
  , traverse_
  ) where

import Control.Prim.Concurrent
import Control.Prim.Concurrent.MVar
import Control.Prim.Exception
import Control.Scheduler.Computation
import Control.Scheduler.Queue
import Control.Scheduler.Types
import Data.Coerce
import qualified Data.Foldable as F (foldl')
import Data.Prim.Array
import Data.Prim.PVar
import Data.Prim.Ref



-- | Initialize a separate state for each worker.
--
-- @since 1.4.0
initWorkerStates :: MonadIO m => Comp -> (WorkerId -> m ws) -> m (WorkerStates ws)
initWorkerStates comp initState = do
  let nWorkers = compNumWorkers comp
  arr <- newRawSBMArray $ Size nWorkers
  let go i =
        when (i < nWorkers) $ do
          state <- initState (WorkerId i)
          writeSBMArray arr i state
          go (i + 1)
  go 0
  workerStates <- freezeSBMArray arr
  mutex <- newRef False
  pure
    WorkerStates
      {_workerStatesComp = comp, _workerStatesArray = workerStates, _workerStatesMutex = mutex}

withSchedulerWSInternal ::
     MonadUnliftIO m
  => (Comp -> (Scheduler m a -> t) -> m b)
  -> WorkerStates s
  -> (SchedulerWS s m a -> t)
  -> m b
withSchedulerWSInternal withScheduler' states action = bracket lockState unlockState runSchedulerWS
  where
    mutex = _workerStatesMutex states
    lockState = atomicModifyRefCAS mutex $ (,) True
    unlockState wasLocked
      | wasLocked = pure ()
      | otherwise = atomicWriteRef mutex False
    runSchedulerWS isLocked
      | isLocked = throw MutexException
      | otherwise =
        withScheduler' (_workerStatesComp states) $ \scheduler ->
          action (SchedulerWS states scheduler)


-- | The most basic scheduler that simply runs the task instead of scheduling it. Early termination
-- requests are bluntly ignored.
--
-- @since 1.1.0
trivialScheduler_ :: Applicative f => Scheduler f ()
trivialScheduler_ =
  Scheduler
    { _numWorkers = 1
    , _scheduleWorkId = \f -> f (WorkerId 0)
    , _terminate = const $ pure ()
    , _waitForCurrentBatch = pure $ Finished []
    , _earlyResults = pure Nothing
    , _currentBatchId = pure $ BatchId 0
    , _cancelBatch = \_ _ -> pure False
    , _batchEarly = pure Nothing
    }


-- | This trivial scheduler will behave in a similar way as
-- `Control.Scheduler.withSchedulerR` with `Seq` computation strategy, except it is
-- restricted to `PrimMonad`, instead of `MonadUnliftIO` and the work isn't scheduled, but
-- rather computed immediately.
--
-- @since 1.4.2
withTrivialSchedulerR :: MonadPrim s m => (Scheduler m a -> m b) -> m (Results a)
withTrivialSchedulerR action = do
  resRef <- newRef []
  batchRef <- newPVar $ BatchId 0
  finResRef <- newRef Nothing
  batchEarlyRef <- newRef Nothing
  let bumpCurrentBatchId = atomicAddPVar_ batchRef (BatchId 1)
      bumpBatchId (BatchId c) =
        atomicModifyPVar batchRef $ \b@(BatchId x) ->
          if x == c
            then (BatchId (x + 1), True)
            else (b, False)
      takeBatchEarly = atomicModifyRef batchEarlyRef $ \mEarly -> (Nothing, mEarly)
      takeResults = atomicModifyRef resRef $ \res -> ([], res)
  _ <-
    action $
    Scheduler
      { _numWorkers = 1
      , _scheduleWorkId =
          \f -> do
            r <- f (WorkerId 0)
            r `seq` atomicModifyRef resRef (\rs -> (r : rs, ()))
      , _terminate =
          \early -> do
            bumpCurrentBatchId
            finishEarly <- collectResults (Just early) takeResults
            unEarly early <$ writeRef finResRef (Just finishEarly)
      , _waitForCurrentBatch = do
          mEarly <- takeBatchEarly
          rs <- collectResults mEarly . pure =<< takeResults
          rs <$ bumpCurrentBatchId
      , _earlyResults = readRef finResRef
      , _currentBatchId = readPVar batchRef
      , _batchEarly = takeBatchEarly
      , _cancelBatch =
          \batchId early -> do
            b <- bumpBatchId batchId
            when b $ writeRef batchEarlyRef (Just early)
            pure b
      }
  readRef finResRef >>= \case
    Just rs -> pure $ reverseResults rs
    Nothing -> do
      mEarly <- takeBatchEarly
      reverseResults <$> collectResults mEarly takeResults



-- | Same as `Control.Scheduler.withTrivialScheduler`, but works in `MonadUnliftIO` and
-- returns results in an original LIFO order.
--
-- @since 1.4.2
withTrivialSchedulerRIO :: MonadUnliftIO m => (Scheduler m a -> m b) -> m (Results a)
withTrivialSchedulerRIO action = do
  resRef <- newRef []
  batchRef <- newPVar $ BatchId 0
  finResRef <- newRef Nothing
  batchEarlyRef <- newRef Nothing
  let bumpCurrentBatchId = atomicAddPVar_ batchRef (coerce (1 :: Int))
      bumpBatchId (BatchId c) =
        atomicModifyPVar batchRef $ \b@(BatchId x) ->
          if x == c
            then (BatchId (x + 1), True)
            else (b, False)
      takeBatchEarly = atomicModifyRef batchEarlyRef $ \mEarly -> (Nothing, mEarly)
      takeResults = atomicModifyRef resRef $ \res -> ([], res)
      scheduler =
        Scheduler
          { _numWorkers = 1
          , _scheduleWorkId =
              \f -> do
                r <- f (WorkerId 0)
                r `seq` atomicModifyRefCAS_ resRef (r :)
          , _terminate =
              \ !early -> do
                bumpCurrentBatchId
                finishEarly <- collectResults (Just early) takeResults
                atomicWriteRef finResRef (Just finishEarly)
                throw TerminateEarlyException
          , _waitForCurrentBatch = do
                mEarly <- takeBatchEarly
                rs <- collectResults mEarly . pure =<< takeResults
                rs <$ bumpCurrentBatchId
          , _earlyResults = readRef finResRef
          , _currentBatchId = readPVar batchRef
          , _batchEarly = takeBatchEarly
          , _cancelBatch =
              \batchId early -> do
                b <- bumpBatchId batchId
                when b $ atomicWriteRef batchEarlyRef (Just early)
                pure b
          }
  _ :: Either TerminateEarlyException b <- try $ action scheduler
  readRef finResRef >>= \case
    Just rs -> pure rs
    Nothing -> do
        mEarly <- takeBatchEarly
        collectResults mEarly takeResults
{-# INLINEABLE withTrivialSchedulerRIO #-}


-- | This is generally a faster way to traverse while ignoring the result rather than using `mapM_`.
--
-- @since 1.0.0
traverse_ :: (Applicative f, Foldable t) => (a -> f ()) -> t a -> f ()
traverse_ f = F.foldl' (\c a -> c *> f a) (pure ())
{-# INLINE traverse_ #-}

scheduleJobs :: MonadIO m => Jobs m a -> (WorkerId -> m a) -> m ()
scheduleJobs = scheduleJobsWith mkJob
{-# INLINEABLE scheduleJobs #-}

-- | Ignores the result of computation, thus avoiding some overhead.
scheduleJobs_ :: MonadIO m => Jobs m a -> (WorkerId -> m b) -> m ()
scheduleJobs_ = scheduleJobsWith (\job -> pure (Job_ (void . job (\_ -> pure ()))))
{-# INLINEABLE scheduleJobs_ #-}

scheduleJobsWith ::
     MonadIO m
  => (((b -> m ()) -> WorkerId -> m ()) -> m (Job m a))
  -> Jobs m a
  -> (WorkerId -> m b)
  -> m ()
scheduleJobsWith mkJob' Jobs {..} action = do
  job <-
    mkJob' $ \storeResult wid -> do
      res <- action wid
      res `seq` storeResult res
  atomicAddPVar_ jobsQueueCount 1
  pushJQueue jobsQueue job
{-# INLINEABLE scheduleJobsWith #-}


-- | Runs the worker until it is terminated with a `WorkerTerminateException` or is killed
-- by some other asynchronous exception, which will propagate to the user calling thread.
runWorker ::
     MonadUnliftIO m
  => (forall c. m c -> m c)
  -> WorkerId
  -> Jobs m a
  -> m ()
runWorker unmask wId Jobs {jobsQueue, jobsQueueCount, jobsSchedulerStatus} = go
  where
    onBlockedMVar eUnblocked =
      case eUnblocked of
        Right () -> go
        Left uExc
          | Just WorkerTerminateException <- asyncExceptionFromException uExc -> return ()
        Left uExc
          | Just CancelBatchException <- asyncExceptionFromException uExc -> go
        Left uExc -> throw uExc
    go = do
      eRes <- try $ do
        job <- popJQueue jobsQueue
        unmask (job wId >> atomicSubFetchOldPVar jobsQueueCount 1)
      -- \ popJQueue can block, but it is still interruptable
      case eRes of
        Right 1 -> try (putMVar jobsSchedulerStatus SchedulerIdle) >>= onBlockedMVar
        Right _ -> go
        Left exc
          | Just WorkerTerminateException <- asyncExceptionFromException exc -> return ()
        Left exc
          | Just CancelBatchException <- asyncExceptionFromException exc -> go
        Left exc -> do
          eUnblocked <-
            try $ putMVar jobsSchedulerStatus (SchedulerWorkerException (WorkerException exc))
          -- \ without blocking with putMVar here we would not be able to report an
          -- exception in the main thread, especially if `exc` is asynchronous.
          unless (isSyncException exc) $ throw exc
          onBlockedMVar eUnblocked
{-# INLINEABLE runWorker #-}


initScheduler ::
     MonadIO m
  => Comp
  -> (Jobs m a -> (WorkerId -> m a) -> m ())
  -> (JQueue m a -> m [a])
  -> m (Jobs m a, [ThreadId] -> Scheduler m a)
initScheduler comp submitWork collect = do
  let jobsNumWorkers = compNumWorkers comp
  jobsQueue <- newJQueue
  jobsQueueCount <- newPVar 1
  jobsSchedulerStatus <- newEmptyMVar
  earlyTerminationResultRef <- newRef Nothing
  batchIdRef <- newPVar $ BatchId 0
  batchEarlyRef <- newRef Nothing
  let jobs =
        Jobs
          { jobsNumWorkers = jobsNumWorkers
          , jobsQueue = jobsQueue
          , jobsQueueCount = jobsQueueCount
          , jobsSchedulerStatus = jobsSchedulerStatus
          }
      bumpCurrentBatchId = atomicAddPVar_ batchIdRef (coerce (1 :: Int))
      bumpBatchId (BatchId c) =
        atomicModifyPVar batchIdRef $ \b@(BatchId x) ->
          if x == c
            then (BatchId (x + 1), True)
            else (b, False)
      mkScheduler tids =
        Scheduler
          { _numWorkers = jobsNumWorkers
          , _scheduleWorkId = submitWork jobs
          , _terminate =
              \early -> do
                finishEarly <-
                  case early of
                    Early r -> FinishedEarly <$> collect jobsQueue <*> pure r
                    EarlyWith r -> pure $ FinishedEarlyWith r
                bumpCurrentBatchId
                atomicWriteRef earlyTerminationResultRef $ Just finishEarly
                throw TerminateEarlyException
          , _waitForCurrentBatch =
              do scheduleJobs_ jobs (\_ -> atomicSubPVar_ jobsQueueCount 1)
                 unblockPopJQueue jobsQueue
                 status <- takeMVar jobsSchedulerStatus
                 mEarly <- atomicModifyRefCAS batchEarlyRef $ (,) Nothing
                 rs <-
                   case status of
                     SchedulerWorkerException (WorkerException exc) ->
                       case fromException exc of
                         Just CancelBatchException -> do
                           _ <- clearPendingJQueue jobsQueue
                           traverse_ (`throwTo` CancelBatchException) tids
                           collectResults mEarly . pure =<< collect jobsQueue
                         Nothing -> throw exc
                     SchedulerIdle -> do
                       blockPopJQueue jobsQueue
                       bumpCurrentBatchId
                       res <- collect jobsQueue
                       res `seq` collectResults mEarly (pure res)
                 rs <$ atomicWritePVar jobsQueueCount 1
          , _earlyResults = readRef earlyTerminationResultRef
          , _currentBatchId = readPVar batchIdRef
          , _batchEarly = readRef batchEarlyRef
          , _cancelBatch =
              \batchId early -> do
                b <- bumpBatchId batchId
                when b $ do
                  blockPopJQueue jobsQueue
                  atomicWriteRef batchEarlyRef $ Just early
                  throw CancelBatchException
                pure b
          }
  pure (jobs, mkScheduler)
{-# INLINEABLE initScheduler #-}

withSchedulerInternal ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Jobs m a -> (WorkerId -> m a) -> m ()) -- ^ How to schedule work
  -> (JQueue m a -> m [a]) -- ^ How to collect results
  -> (Scheduler m a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m (Results a)
withSchedulerInternal comp submitWork collect onScheduler = do
  (jobs@Jobs {..}, mkScheduler) <- initScheduler comp submitWork collect
  -- / Wait for the initial jobs to get scheduled before spawining off the workers, otherwise it
  -- would be trickier to identify the beginning and the end of a job pool.
  withRunInIO $ \run -> do
    bracket (run (spawnWorkers jobs comp)) terminateWorkers $ \tids ->
      let scheduler = mkScheduler tids
          readEarlyTermination =
            _earlyResults scheduler >>= \case
              Nothing -> error "Impossible: uninitialized early termination value"
              Just rs -> pure rs
       in try (run (onScheduler scheduler)) >>= \case
            Left TerminateEarlyException -> run readEarlyTermination
            Right _ -> do
              run $ scheduleJobs_ jobs (\_ -> atomicSubPVar_ jobsQueueCount 1)
              run $ unblockPopJQueue jobsQueue
              status <- takeMVar jobsSchedulerStatus
                -- \ wait for all worker to finish. If any one of the workers had a problem, then
                -- this MVar will contain an exception
              case status of
                SchedulerWorkerException (WorkerException exc)
                  | Just TerminateEarlyException <- fromException exc -> run readEarlyTermination
                  | Just CancelBatchException <- fromException exc ->
                    run $ do
                      mEarly <- _batchEarly scheduler
                      collectResults mEarly (collect jobsQueue)
                  | otherwise -> throw exc
                  -- \ Here we need to unwrap the legit worker exception and rethrow it, so
                  -- the main thread will think like it's his own
                SchedulerIdle ->
                  run $ do
                    mEarly <- _batchEarly scheduler
                    collectResults mEarly (collect jobsQueue)
                  -- \ Now we are sure all workers have done their job we can safely read
                  -- all of the Refs with results
{-# INLINEABLE withSchedulerInternal #-}


collectResults :: Applicative f => Maybe (Early a) -> f [a] -> f (Results a)
collectResults mEarly collect =
  case mEarly of
    Nothing            -> Finished <$> collect
    Just (Early r)     -> FinishedEarly <$> collect <*> pure r
    Just (EarlyWith r) -> pure $ FinishedEarlyWith r
{-# INLINEABLE collectResults #-}


spawnWorkers :: forall m a. MonadUnliftIO m => Jobs m a -> Comp -> m [ThreadId]
spawnWorkers jobs@Jobs {jobsNumWorkers} =
  \case
    Par      -> spawnWorkersWith forkOn [1 .. jobsNumWorkers]
    ParOn ws -> spawnWorkersWith forkOn ws
    ParN _   -> spawnWorkersWith (\_ -> fork) [1 .. jobsNumWorkers]
    Seq      -> spawnWorkersWith (\_ -> fork) [1 :: Int]
    -- \ sequential computation is suboptimal when used in this way.
  where
    spawnWorkersWith forker ws =
      forM (zip [0 ..] ws) $ \(wId, on) ->
        mask $ \restore ->
          forker on $ runWorker restore wId jobs
{-# INLINEABLE spawnWorkers #-}

terminateWorkers :: MonadPrim RW m => [ThreadId] -> m ()
terminateWorkers = traverse_ (`throwTo` WorkerTerminateException)

-- | Conversion to a list. Elements are expected to be in the orignal LIFO order, so
-- calling `reverse` is still necessary for getting the results in FIFO order.
resultsToList :: Results a -> [a]
resultsToList = \case
  Finished rs         -> rs
  FinishedEarly rs r  -> r:rs
  FinishedEarlyWith r -> [r]
{-# INLINEABLE resultsToList #-}


reverseResults :: Results a -> Results a
reverseResults = \case
  Finished rs        -> Finished (reverse rs)
  FinishedEarly rs r -> FinishedEarly (reverse rs) r
  res                -> res
{-# INLINEABLE reverseResults #-}

