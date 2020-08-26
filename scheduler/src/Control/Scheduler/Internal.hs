{-# LANGUAGE BangPatterns #-}
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
  , safeBracketOnError
  ) where

import Data.Coerce
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Unlift
import Control.Scheduler.Computation
import Control.Scheduler.Types
import Control.Scheduler.Queue
import Data.Atomics (atomicModifyIORefCAS, atomicModifyIORefCAS_)
import qualified Data.Foldable as F (foldl')
import Data.IORef
import Data.Primitive.SmallArray
import Data.Primitive.MutVar
import Data.Primitive.PVar



-- | Initialize a separate state for each worker.
--
-- @since 1.4.0
initWorkerStates :: MonadIO m => Comp -> (WorkerId -> m s) -> m (WorkerStates s)
initWorkerStates comp initState = do
  nWorkers <- getCompWorkers comp
  arr <- liftIO $ newSmallArray nWorkers (error "Uninitialized")
  let go i =
        when (i < nWorkers) $ do
          state <- initState (WorkerId i)
          liftIO $ writeSmallArray arr i state
          go (i + 1)
  go 0
  workerStates <- liftIO $ unsafeFreezeSmallArray arr
  mutex <- liftIO $ newIORef False
  pure
    WorkerStates
      {_workerStatesComp = comp, _workerStatesArray = workerStates, _workerStatesMutex = mutex}

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
    , _waitForBatch = pure $ Finished []
    , _earlyResults = pure Nothing
    , _currentBatchId = pure $ BatchId 0
    , _cancelCurrentBatch = const $ pure ()
    , _batchEarly = pure Nothing
    }


-- | This trivial scheduler will behave in a similar way as `withSchedulerR` with `Seq`
-- computation strategy, except it is restricted to `PrimMonad`, instead of
-- `MonadUnliftIO` and the work isn't scheduled, but rather computed immediately.
--
-- @since 1.4.2
withTrivialSchedulerR :: PrimMonad m => (Scheduler m a -> m b) -> m (Results a)
withTrivialSchedulerR action = do
  resVar <- newMutVar []
  batchVar <- newMutVar $ BatchId 0
  finResVar <- newMutVar Nothing
  batchEarlyVar <- newMutVar Nothing
  let bumpBatchId = atomicModifyMutVar' batchVar (\(BatchId x) -> (BatchId (x + 1), ()))
      takeBatchEarly = atomicModifyMutVar' batchEarlyVar $ \mEarly -> (Nothing, mEarly)
      takeResults = atomicModifyMutVar' resVar $ \res -> ([], res)
  _ <-
    action $
    Scheduler
      { _numWorkers = 1
      , _scheduleWorkId =
          \f -> do
            r <- f (WorkerId 0)
            r `seq` bumpBatchId >> atomicModifyMutVar' resVar (\rs -> (r : rs, ()))
      , _terminate =
          \early -> do
            finishEarly <- collectResults (Just early) takeResults
            unEarly early <$ bumpBatchId <* writeMutVar finResVar (Just finishEarly)
      , _waitForBatch =
          do bumpBatchId
             mEarly <- takeBatchEarly
             collectResults mEarly . pure =<< takeResults
      , _earlyResults = readMutVar finResVar
      , _currentBatchId = readMutVar batchVar
      , _batchEarly = takeBatchEarly
      , _cancelCurrentBatch = \early -> writeMutVar batchEarlyVar (Just early) >> bumpBatchId
      }
  readMutVar finResVar >>= \case
    Just rs -> pure $ reverseResults rs
    Nothing -> do
      mEarly <- takeBatchEarly
      reverseResults <$> collectResults mEarly takeResults



-- | Same as `withTrivialSchedulerIO`, but works in MonadIO and returns results in an
-- original LIFO order.
withTrivialSchedulerRIO :: MonadUnliftIO m => (Scheduler m a -> m b) -> m (Results a)
withTrivialSchedulerRIO action = do
  resRef <- liftIO $ newIORef []
  batchRef <- liftIO $ newIORef $ BatchId 0
  finResRef <- liftIO $ newIORef Nothing
  batchEarlyRef <- liftIO $ newIORef Nothing
  let bumpBatchId = atomicModifyIORefCAS_ (coerce batchRef) (+ (1 :: Int))
      takeBatchEarly = atomicModifyIORefCAS batchEarlyRef $ \mEarly -> (Nothing, mEarly)
      takeResults = atomicModifyIORefCAS resRef $ \res -> ([], res)
      scheduler =
        Scheduler
          { _numWorkers = 1
          , _scheduleWorkId =
              \f -> do
                r <- f (WorkerId 0)
                r `seq` liftIO (atomicModifyIORefCAS_ resRef (r :))
          , _terminate =
              \ !early ->
                liftIO $ do
                  finishEarly <- collectResults (Just early) takeResults
                  bumpBatchId <* writeIORef finResRef (Just finishEarly)
                  throwIO TerminateEarlyException
          , _waitForBatch =
              liftIO $ do
                bumpBatchId
                mEarly <- takeBatchEarly
                collectResults mEarly . pure =<< takeResults
          , _earlyResults = liftIO (readIORef finResRef)
          , _currentBatchId = liftIO (readIORef batchRef)
          , _batchEarly = liftIO takeBatchEarly
          , _cancelCurrentBatch =
              \early -> liftIO (writeIORef batchEarlyRef (Just early) >> bumpBatchId)
          }
  _ :: Either TerminateEarlyException b <- withRunInIO $ \run -> try $ run $ action scheduler
  liftIO (readIORef finResRef) >>= \case
    Just rs -> pure rs
    Nothing ->
      liftIO $ do
        mEarly <- takeBatchEarly
        collectResults mEarly takeResults


-- | This is generally a faster way to traverse while ignoring the result rather than using `mapM_`.
--
-- @since 1.0.0
traverse_ :: (Applicative f, Foldable t) => (a -> f ()) -> t a -> f ()
traverse_ f = F.foldl' (\c a -> c *> f a) (pure ())


scheduleJobs :: MonadIO m => Jobs m a -> (WorkerId -> m a) -> m ()
scheduleJobs = scheduleJobsWith mkJob

-- | Ignores the result of computation, thus avoiding some overhead.
scheduleJobs_ :: MonadIO m => Jobs m a -> (WorkerId -> m b) -> m ()
scheduleJobs_ = scheduleJobsWith (\job -> pure (Job_ (void . job (\_ -> pure ()))))

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
  liftIO $ void $ atomicAddIntPVar jobsQueueCount 1
  pushJQueue jobsQueue job


-- | Runs the worker until it is terminated with a `WorkerTerminateException` or is killed
-- by some other asynchronous exception, which will propagate to the user calling thread.
runWorker ::
     MonadUnliftIO m
  => (forall b. m b -> IO b)
  -> (forall c. IO c -> IO c)
  -> WorkerId
  -> Jobs m a
  -> IO ()
runWorker run unmask wId Jobs {jobsQueue, jobsQueueCount, jobsSchedulerStatus} = go
  where
    dropCount = atomicSubIntPVar jobsQueueCount 1
    onBlockedMVar eUnblocked =
      case eUnblocked of
        Right () -> go
        Left uExc
          | Just WorkerTerminateException <- asyncExceptionFromException uExc -> return ()
        Left uExc
          | Just CancelBatchException <- asyncExceptionFromException uExc -> go
        Left uExc -> throwIO uExc
    go = do
      eRes <- try $ do
        job <- run (popJQueue jobsQueue)
        unmask (run (job wId) >> dropCount)
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
          unless (isSyncException exc) $ throwIO exc
          onBlockedMVar eUnblocked



initScheduler ::
     MonadIO m
  => Comp
  -> (Jobs m a -> (WorkerId -> m a) -> m ())
  -> (JQueue m a -> m [a])
  -> m (Jobs m a, [ThreadId] -> Scheduler m a)
initScheduler comp submitWork collect = do
  jobsNumWorkers <- getCompWorkers comp
  jobsQueue <- newJQueue
  jobsQueueCount <- liftIO $ newPVar 1
  jobsSchedulerStatus <- liftIO newEmptyMVar
  earlyTerminationResultRef <- liftIO $ newIORef Nothing
  batchIdRef <- liftIO $ newIORef $ BatchId 0
  batchEarlyRef <- liftIO $ newIORef Nothing
  let jobs =
        Jobs
          { jobsNumWorkers = jobsNumWorkers
          , jobsQueue = jobsQueue
          , jobsQueueCount = jobsQueueCount
          , jobsSchedulerStatus = jobsSchedulerStatus
          }
      bumpBatchId = atomicModifyIORef' (coerce batchIdRef) (\x -> ((x + (1 :: Int)), ()))
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
                liftIO $ do
                  bumpBatchId
                  writeIORef earlyTerminationResultRef $ Just finishEarly
                  throwIO TerminateEarlyException
          , _waitForBatch =
              do scheduleJobs_ jobs (\_ -> liftIO $ void $ atomicSubIntPVar jobsQueueCount 1)
                 unblockPopJQueue jobsQueue
                 status <- liftIO $ takeMVar jobsSchedulerStatus
                 mEarly <- liftIO $ atomicModifyIORef' batchEarlyRef $ \mEarly -> (Nothing, mEarly)
                 rs <-
                   case status of
                     SchedulerWorkerException (WorkerException exc) ->
                       case fromException exc of
                         Just CancelBatchException -> do
                           _ <- clearPendingJQueue jobsQueue
                           liftIO $
                             traverse_ (`throwTo` SomeAsyncException CancelBatchException) tids
                           collectResults mEarly . pure =<< collect jobsQueue
                         Nothing -> liftIO $ throwIO exc
                     SchedulerIdle -> do
                       blockPopJQueue jobsQueue
                       liftIO bumpBatchId
                       res <- collect jobsQueue
                       res `seq` collectResults mEarly (pure res)
                 rs <$ liftIO (atomicWriteIntPVar jobsQueueCount 1)
          , _earlyResults = liftIO (readIORef earlyTerminationResultRef)
          , _currentBatchId = liftIO (readIORef batchIdRef)
          , _batchEarly = liftIO (readIORef batchEarlyRef)
          , _cancelCurrentBatch =
              \early -> do
                blockPopJQueue jobsQueue
                liftIO $ do
                  writeIORef batchEarlyRef $ Just early
                  bumpBatchId
                  throwIO CancelBatchException
          }
  pure (jobs, mkScheduler)

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
  safeBracketOnError (spawnWorkers jobs comp) terminateWorkers $ \tids ->
    let scheduler = mkScheduler tids
        readEarlyTermination =
          _earlyResults scheduler >>= \case
            Nothing -> error "Impossible: uninitialized early termination value"
            Just rs -> pure rs
     in withRunInIO $ \run -> do
          eEarlyTermination <- try $ run $ onScheduler scheduler
          case eEarlyTermination of
            Left TerminateEarlyException -> run $ terminateWorkers tids >> readEarlyTermination
            Right _ -> do
              run $ scheduleJobs_ jobs (\_ -> liftIO $ void $ atomicSubIntPVar jobsQueueCount 1)
              run $ unblockPopJQueue jobsQueue
              status <- takeMVar jobsSchedulerStatus
              -- \ wait for all worker to finish. If any one of the workers had a problem, then
              -- this MVar will contain an exception
              run $ terminateWorkers tids
              case status of
                SchedulerWorkerException (WorkerException exc)
                  | Just TerminateEarlyException <- fromException exc -> run readEarlyTermination
                  | Just CancelBatchException <- fromException exc ->
                    run $ do
                      mEarly <- _batchEarly scheduler
                      collectResults mEarly (collect jobsQueue)
                  | otherwise -> throwIO exc
                -- \ Here we need to unwrap the legit worker exception and rethrow it, so
                -- the main thread will think like it's his own
                SchedulerIdle ->
                  run $ do
                    mEarly <- _batchEarly scheduler
                    collectResults mEarly (collect jobsQueue)
                -- \ Now we are sure all workers have done their job we can safely read
                -- all of the IORefs with results


collectResults :: Applicative f => Maybe (Early a) -> f [a] -> f (Results a)
collectResults mEarly collect =
  case mEarly of
    Nothing -> Finished <$> collect
    Just (Early r) -> FinishedEarly <$> collect <*> pure r
    Just (EarlyWith r) -> pure $ FinishedEarlyWith r


spawnWorkers :: forall m a. MonadUnliftIO m => Jobs m a -> Comp -> m [ThreadId]
spawnWorkers jobs@Jobs {jobsNumWorkers} =
  \case
    Par -> spawnWorkersWith forkOnWithUnmask [1 .. jobsNumWorkers]
    ParOn ws -> spawnWorkersWith forkOnWithUnmask ws
    ParN _ -> spawnWorkersWith (\_ -> forkIOWithUnmask) [1 .. jobsNumWorkers]
    Seq -> spawnWorkersWith (\_ -> forkIOWithUnmask) [1 :: Int]
    -- \ sequential computation is suboptimal when used in this way.
  where
    spawnWorkersWith ::
         MonadUnliftIO m
      => (Int -> ((forall c. IO c -> IO c) -> IO ()) -> IO ThreadId)
      -> [Int]
      -> m [ThreadId]
    spawnWorkersWith fork ws =
      withRunInIO $ \run ->
        forM (zip [0 ..] ws) $ \(wId, on) ->
          fork on $ \unmask -> runWorker run unmask wId jobs

terminateWorkers :: MonadIO m => [ThreadId] -> m ()
terminateWorkers = liftIO . traverse_ (`throwTo` SomeAsyncException WorkerTerminateException)

-- | Conversion to a list. Elements are expected to be in the orignal LIFO order, so
-- calling `reverse` is still necessary for getting the results in FIFO order.
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
