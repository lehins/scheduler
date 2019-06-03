{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternSynonyms #-}
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
  , WorkerId(..)
  , numWorkers
  , scheduleWork
  , scheduleWork_
  , scheduleWorkId
  , scheduleWorkId_
  , terminate
  , terminate_
  , terminateWith
  -- ** Stateful Workers
  , WorkerStates
  , initWorkerStates
  , SchedulerS
  , withoutStates
  , scheduleWorkState
  , scheduleWorkState_
  -- * Initialize Scheduler
  , withScheduler
  , withScheduler_
  , trivialScheduler_
  -- ** Stateful
  , withSchedulerS
  , withSchedulerS_
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
  ) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Unlift
import Control.Scheduler.Computation
import Control.Scheduler.Queue
import Data.Atomics (atomicModifyIORefCAS, atomicModifyIORefCAS_)
import qualified Data.Foldable as F (foldl', traverse_)
import Data.IORef
import Data.Maybe (catMaybes)
import Data.Primitive.Array
import Data.Traversable


data Jobs m a = Jobs
  { jobsNumWorkers :: {-# UNPACK #-} !Int
  , jobsQueue      :: !(JQueue m a)
  , jobsCountRef   :: !(IORef Int)
  }

-- | Main type for scheduling work. See `withScheduler` or `withScheduler_` for ways to construct
-- and use this data type.
--
-- @since 1.0.0
data Scheduler m a = Scheduler
  { _numWorkers     :: {-# UNPACK #-} !Int
  , _scheduleWorkId :: (Int -> m a) -> m ()
  , _terminate      :: a -> m a
  , _terminateWith  :: a -> m a
  }

-- | This is a wrapper around `Scheduler`, that keeps a separate state for each
-- individual worker. A good example of this would be using a separate random number
-- generator for each worker since most of the time generators are not thread safe.
--
-- @since 1.4.0
data SchedulerS s m a = SchedulerS
  { _workerStates :: !(WorkerStates s)
  , _getScheduler :: !(Scheduler m a)
  }

-- | Get the scheduler that can't access worker states.
--
-- @since 1.4.0
withoutStates :: SchedulerS s m a -> Scheduler m a
withoutStates = _getScheduler

-- | A unique id for the worker in the `Scheduler` context. It will always be a number
-- from @0@ up to, but not including, the number of workers a scheduler has, which can
-- always be determined by `numWorkers`.
--
-- @since 1.4.0
newtype WorkerId = WorkerId
  { getWorkerId :: Int
  } deriving (Show, Eq, Ord, Enum)


-- | Each worker is capable of keeping it's own state, that can be share for different
-- schedulers, but not at the same time. In other words using same `WorkerStates` on
-- `withSchedulerS` concurrently will result in an error. Can be initialized with
-- `initWorkerStates`
--
-- @since 1.4.0
data WorkerStates s = WorkerStates
  { _workerStatesComp  :: !Comp
  , _workerStatesArray :: !(Array s)
  , _workerStatesMutex :: !(IORef Bool)
  }

-- | Initilize a separate state for each worker.
--
-- @since 1.4.0
initWorkerStates :: MonadIO m => Comp -> (WorkerId -> m s) -> m (WorkerStates s)
initWorkerStates comp initState = do
  nWorkers <- getCompWorkers comp
  workerStates <- mapM (initState . WorkerId) [0 .. nWorkers - 1]
  mutex <- liftIO $ newIORef False
  pure
    WorkerStates
      { _workerStatesComp = comp
      , _workerStatesArray = fromListN nWorkers workerStates
      , _workerStatesMutex = mutex
      }

-- | Run a scheduler with stateful workers
--
-- @since 1.4.0
withSchedulerS :: MonadUnliftIO m => WorkerStates s -> (SchedulerS s m a -> m b) -> m [a]
withSchedulerS states action =
  withRunInIO $ \run -> bracket lockState unlockState (run . runSchedulerS)
  where
    mutex = _workerStatesMutex states
    lockState = atomicModifyIORef' mutex ((,) True)
    unlockState wasLocked
      | wasLocked = pure ()
      | otherwise = writeIORef mutex False
    runSchedulerS isLocked
      | isLocked =
        error "withSchedulerS: WorkerStates cannot be used at the same time by different schedulers"
      | otherwise =
        withScheduler (_workerStatesComp states) $ \scheduler ->
          action (SchedulerS states scheduler)

-- | Run a scheduler with stateful workers, while discarding computation results.
--
-- @since 1.4.0
withSchedulerS_ :: MonadUnliftIO m => WorkerStates s -> (SchedulerS s m () -> m b) -> m ()
withSchedulerS_ states = void . withSchedulerS states


-- | Schedule a job that will get a worker state passed as an argument
--
-- @since 1.4.0
scheduleWorkState :: SchedulerS s m a -> (s -> m a) -> m ()
scheduleWorkState schedulerS withState =
  scheduleWorkId (_getScheduler schedulerS) $ \i ->
    withState (indexArray (_workerStatesArray (_workerStates schedulerS)) i)

-- | Same as `scheduleWorkState`, but dont' keep the result of computation.
--
-- @since 1.4.0
scheduleWorkState_ :: SchedulerS s m () -> (s -> m ()) -> m ()
scheduleWorkState_ schedulerS withState =
  scheduleWorkId_ (_getScheduler schedulerS) $ \i ->
    withState (indexArray (_workerStatesArray (_workerStates schedulerS)) i)


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
scheduleWorkId :: Scheduler m a -> (Int -> m a) -> m ()
scheduleWorkId =_scheduleWorkId

-- | As soon as possible try to terminate any computation that is being performed by all workers
-- managed by this scheduler and collect whatever results have been computed, with supplied
-- element guaranteed to being the last one.
--
-- /Important/ - With `Seq` strategy this will not stop other scheduled tasks from being computed,
-- although it will make sure their results are discarded.
--
-- @since 1.1.0
terminate :: Scheduler m a -> a -> m a
terminate = _terminate

-- | Same as `terminate`, but returning a single element list containing the supplied
-- argument. This can be very useful for parallel search algorithms.
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

-- | Same as `scheduleWork`, but only for a `Scheduler` that doesn't keep the results.
--
-- @since 1.1.0
scheduleWork_ :: Scheduler m () -> m () -> m ()
scheduleWork_ = scheduleWork

-- | Same as `scheduleWorkId`, but only for a `Scheduler` that doesn't keep the results.
--
-- @since 1.2.0
scheduleWorkId_ :: Scheduler m () -> (Int -> m ()) -> m ()
scheduleWorkId_ = _scheduleWorkId

-- | Similar to `terminate`, but for a `Scheduler` that does not keep any results of computation.
--
-- /Important/ - In case of `Seq` computation strategy this function has no affect.
--
-- @since 1.1.0
terminate_ :: Scheduler m () -> m ()
terminate_ = (`_terminateWith` ())

-- | The most basic scheduler that simply runs the task instead of scheduling it. Early termination
-- requests are simply ignored.
--
-- @since 1.1.0
trivialScheduler_ :: Applicative f => Scheduler f ()
trivialScheduler_ = Scheduler
  { _numWorkers = 1
  , _scheduleWorkId = \f -> f 0
  , _terminate = const $ pure ()
  , _terminateWith = const $ pure ()
  }


data SchedulerOutcome a
  = SchedulerFinished
  | SchedulerTerminatedEarly ![a]
  | SchedulerWorkerException WorkerException



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
    withR _      _ = error "Impossible<traverseConcurrently> - Mismatched sizes"

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


scheduleJobs :: MonadIO m => Jobs m a -> (Int -> m a) -> m ()
scheduleJobs = scheduleJobsWith mkJob

-- | Similarly to `scheduleWork`, but ignores the result of computation, thus having less overhead.
--
-- @since 1.0.0
scheduleJobs_ :: MonadIO m => Jobs m a -> (Int -> m b) -> m ()
scheduleJobs_ = scheduleJobsWith (\job -> pure (Job_ (void . job)))

scheduleJobsWith :: MonadIO m => ((Int -> m b) -> m (Job m a)) -> Jobs m a -> (Int -> m b) -> m ()
scheduleJobsWith mkJob' jobs action = do
  liftIO $ atomicModifyIORefCAS_ (jobsCountRef jobs) (+ 1)
  job <-
    mkJob' $ \ i -> do
      res <- action i
      res `seq`
        dropCounterOnZero (jobsCountRef jobs) $
        retireWorkersN (jobsQueue jobs) (jobsNumWorkers jobs)
      return res
  pushJQueue (jobsQueue jobs) job

-- | Helper function to place required number of @Retire@ instructions on the job queue.
retireWorkersN :: MonadIO m => JQueue m a -> Int -> m ()
retireWorkersN jobsQueue n = traverse_ (pushJQueue jobsQueue) $ replicate n Retire

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


-- | Runs the worker until the job queue is exhausted, at which point it will execute the final task
-- of retirement and return
runWorker :: MonadIO m =>
             Int
          -> JQueue m a
          -> m () -- ^ Action to run upon retirement
          -> m ()
runWorker wId jQueue onRetire = go
  where
    go =
      popJQueue jQueue >>= \case
        Just job -> job wId >> go
        Nothing -> onRetire


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
-- * __Warning__ It is pretty dangerous to schedule jobs that do blocking `IO`, since it can easily
--   lead to deadlock, if you are not careful. Consider this example. First execution works fine,
--   since there are two scheduled workers, and one can unblock the other, but the second scenario
--   immediately results in a deadlock.
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
withScheduler comp = withSchedulerInternal comp scheduleJobs readResults reverse


-- | Same as `withScheduler`, but discards results of submitted jobs.
--
-- @since 1.0.0
withScheduler_ ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Scheduler m a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m ()
withScheduler_ comp = void . withSchedulerInternal comp scheduleJobs_ (const (pure [])) id

withSchedulerInternal ::
     MonadUnliftIO m
  => Comp -- ^ Computation strategy
  -> (Jobs m a -> (Int -> m a) -> m ()) -- ^ How to schedule work
  -> (JQueue m a -> m [Maybe a]) -- ^ How to collect results
  -> ([a] -> [a]) -- ^ Adjust results in some way
  -> (Scheduler m a -> m b)
     -- ^ Action that will be scheduling all the work.
  -> m [a]
withSchedulerInternal comp submitWork collect adjust onScheduler = do
  jobsNumWorkers <- getCompWorkers comp
  sWorkersCounterRef <- liftIO $ newIORef jobsNumWorkers
  jobsQueue <- newJQueue
  jobsCountRef <- liftIO $ newIORef 0
  workDoneMVar <- liftIO newEmptyMVar
  let jobs = Jobs {..}
      scheduler =
        Scheduler
          { _numWorkers = jobsNumWorkers
          , _scheduleWorkId = submitWork jobs
          , _terminate =
              \a -> do
                mas <- collect jobsQueue
                let as = adjust (a : catMaybes mas)
                liftIO $ void $ tryPutMVar workDoneMVar $ SchedulerTerminatedEarly as
                pure a
          , _terminateWith =
              \a -> do
                liftIO $ void $ tryPutMVar workDoneMVar $ SchedulerTerminatedEarly [a]
                pure a
          }
      onRetire =
        dropCounterOnZero sWorkersCounterRef $
        void $ liftIO (tryPutMVar workDoneMVar SchedulerFinished)
  -- / Wait for the initial jobs to get scheduled before spawining off the workers, otherwise it
  -- would be trickier to identify the beginning and the end of a job pool.
  _ <- onScheduler scheduler
  -- / Ensure at least something gets scheduled, so retirement can be triggered
  jc <- liftIO $ readIORef jobsCountRef
  when (jc == 0) $ scheduleJobs_ jobs (\_ -> pure ())
  let spawnWorkersWith fork ws =
        withRunInIO $ \run ->
          forM (zip [0 ..] ws) $ \(wId, on) ->
            fork on $ \unmask ->
              catch
                (unmask $ run $ runWorker wId jobsQueue onRetire)
                (run . handleWorkerException jobsQueue workDoneMVar jobsNumWorkers)
      spawnWorkers =
        case comp of
          Seq -> return []
            -- \ no need to fork threads for a sequential computation
          Par -> spawnWorkersWith forkOnWithUnmask [1 .. jobsNumWorkers]
          ParOn ws -> spawnWorkersWith forkOnWithUnmask ws
          ParN _ -> spawnWorkersWith (\_ -> forkIOWithUnmask) [1 .. jobsNumWorkers]
      terminateWorkers = liftIO . traverse_ (`throwTo` SomeAsyncException WorkerTerminateException)
      doWork tids = do
        when (comp == Seq) $ runWorker 0 jobsQueue onRetire
        mExc <- liftIO $ readMVar workDoneMVar
        -- \ wait for all worker to finish. If any one of them had a problem this MVar will
        -- contain an exception
        case mExc of
          SchedulerFinished -> adjust . catMaybes <$> collect jobsQueue
          -- \ Now we are sure all workers have done their job we can safely read all of the
          -- IORefs with results
          SchedulerTerminatedEarly as -> terminateWorkers tids >> pure as
          SchedulerWorkerException (WorkerException exc) -> liftIO $ throwIO exc
          -- \ Here we need to unwrap the legit worker exception and rethrow it, so the main thread
          -- will think like it's his own
  safeBracketOnError spawnWorkers terminateWorkers doWork


-- | Specialized exception handler for the work scheduler.
handleWorkerException ::
  MonadIO m => JQueue m a -> MVar (SchedulerOutcome a) -> Int -> SomeException -> m ()
handleWorkerException jQueue workDoneMVar nWorkers exc =
  case asyncExceptionFromException exc of
    Just WorkerTerminateException -> return ()
      -- \ some co-worker died, we can just move on with our death.
    _ -> do
      _ <- liftIO $ tryPutMVar workDoneMVar $ SchedulerWorkerException $ WorkerException exc
      -- \ Main thread must know how we died
      -- / Do the co-worker cleanup
      retireWorkersN jQueue (nWorkers - 1)


-- | This exception should normally be never seen in the wild and is for internal use only.
newtype WorkerException =
  WorkerException SomeException
  -- ^ One of workers experienced an exception, main thread will receive the same `SomeException`.
  deriving (Show)

instance Exception WorkerException

data WorkerTerminateException =
  WorkerTerminateException
  -- ^ When a brother worker dies of some exception, all the other ones will be terminated
  -- asynchronously with this one.
  deriving (Show)


instance Exception WorkerTerminateException

-- Copy from unliftio:
safeBracketOnError :: MonadUnliftIO m => m a -> (a -> m b) -> (a -> m c) -> m c
safeBracketOnError before after thing = withRunInIO $ \run -> mask $ \restore -> do
  x <- run before
  res1 <- try $ restore $ run $ thing x
  case res1 of
    Left (e1 :: SomeException) -> do
      _ :: Either SomeException b <-
        try $ uninterruptibleMask_ $ run $ after x
      throwIO e1
    Right y -> return y

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
