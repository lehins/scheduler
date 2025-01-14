{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE Unsafe #-}
{-# OPTIONS_HADDOCK hide, not-home #-}
-- |
-- Module      : Control.Scheduler.Types
-- Copyright   : (c) Alexey Kuleshevich 2018-2020
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Types
  ( Scheduler(..)
  , WorkerStates(..)
  , SchedulerWS(..)
  , GlobalScheduler(..)
  , Batch(..)
  , BatchId(..)
  , Jobs(..)
  , Early(..)
  , unEarly
  , Results(..)
  , SchedulerStatus(..)
  , WorkerException(..)
  , CancelBatchException(..)
  , TerminateEarlyException(..)
  , WorkerTerminateException(..)
  , MutexException(..)
  ) where

import Control.Concurrent (ThreadId)
import Control.Concurrent.MVar
import Control.Exception
import Control.Scheduler.Computation
import Control.Scheduler.Queue
import Data.Functor.Classes
import Data.IORef
import Data.Primitive.SmallArray
import Data.Primitive.PVar

-- | Computed results of scheduled jobs.
--
-- @since 1.4.2
data Results a
  = Finished [a]
  -- ^ Finished normally with all scheduled jobs completed
  | FinishedEarly [a] !a
  -- ^ Finished early by the means of `Control.Scheduler.cancelBatch` or
  -- `Control.Scheduler.terminate`.
  | FinishedEarlyWith !a
  -- ^ Finished early by the means of `Control.Scheduler.cancelBatchWith` or
  -- `Control.Scheduler.terminateWith`.
  deriving (Show, Read, Eq)

instance Functor Results where
  fmap f =
    \case
      Finished xs -> Finished (fmap f xs)
      FinishedEarly xs x -> FinishedEarly (fmap f xs) (f x)
      FinishedEarlyWith x -> FinishedEarlyWith (f x)

instance Foldable Results where
  foldr f acc =
    \case
      Finished xs -> foldr f acc xs
      FinishedEarly xs x -> foldr f (f x acc) xs
      FinishedEarlyWith x -> f x acc
  foldr1 f =
    \case
      Finished xs -> foldr1 f xs
      FinishedEarly xs x -> foldr f x xs
      FinishedEarlyWith x -> x

instance Traversable Results where
  traverse f =
    \case
      Finished xs -> Finished <$> traverse f xs
      FinishedEarly xs x -> FinishedEarly <$> traverse f xs <*> f x
      FinishedEarlyWith x -> FinishedEarlyWith <$> f x

instance Eq1 Results where
  liftEq f (Finished xs1) (Finished xs2) = liftEq f xs1 xs2
  liftEq f (FinishedEarly xs1 x1) (FinishedEarly xs2 x2) = liftEq f xs1 xs2 && f x1 x2
  liftEq f (FinishedEarlyWith x1) (FinishedEarlyWith x2) = f x1 x2
  liftEq _ _ _ = False

instance Show1 Results where
  liftShowsPrec f g n = \case
    Finished xs -> wrap (("Finished " ++) . g xs)
    FinishedEarly xs x -> wrap (("FinishedEarly " ++) . g xs . (" " ++) . f 11 x)
    FinishedEarlyWith x -> wrap (("FinishedEarlyWith " ++) . f 11 x)
    where
      wrap s
        | n <= 1 = s
        | otherwise = ('(' :) . s . (++ ")")

data Jobs m a = Jobs
  { jobsNumWorkers       :: {-# UNPACK #-} !Int
  , jobsQueue            :: !(JQueue m a)
#if MIN_VERSION_pvar(1,0,0)
  , jobsQueueCount       :: !(PVar Int RealWorld)
#else
  , jobsQueueCount       :: !(PVar IO Int)
#endif
  , jobsSchedulerStatus  :: !(MVar SchedulerStatus)
  }


-- | This is a result for premature ending of computation.
data Early a
  = Early a
  -- ^ This value along with all results computed up to the moment when computation was
  -- cancelled or termianted will be returned
  | EarlyWith a
  -- ^ Only this value will be returned all other results will get discarded

unEarly :: Early a -> a
unEarly (Early r) = r
unEarly (EarlyWith r) = r

-- | Main type for scheduling work. See `Control.Scheduler.withScheduler` or
-- `Control.Scheduler.withScheduler_` for ways to construct and use this data type.
--
-- @since 1.0.0
data Scheduler s a = Scheduler
  { _numWorkers          :: {-# UNPACK #-} !Int
  , _scheduleWorkId      :: (WorkerId -> ST s a) -> ST s ()
  , _terminate           :: Early a -> ST s a
  , _waitForCurrentBatch :: ST s (Results a)
  , _earlyResults        :: ST s (Maybe (Results a))
  , _currentBatchId      :: ST s BatchId
  -- ^ Returns an opaque identifier for current batch of jobs, which can be used to either
  -- cancel the batch early or simply check if the batch has finished or not.
  , _cancelBatch         :: BatchId -> Early a -> ST s Bool
  -- ^ Stops current batch and cancells all the outstanding jobs and the ones that are
  -- currently in progress.
  , _batchEarly          :: ST s (Maybe (Early a))
  }


-- | This is a wrapper around `Scheduler`, but it also keeps a separate state for each
-- individual worker. See `Control.Scheduler.withSchedulerWS` or
-- `Control.Scheduler.withSchedulerWS_` for ways to construct and use this data type.
--
-- @since 1.4.0
data SchedulerWS ws a = SchedulerWS
  { _workerStates :: !(WorkerStates ws)
  , _getScheduler :: !(Scheduler RealWorld a)
  }

-- | Each worker is capable of keeping it's own state, that can be share for different
-- schedulers, but not at the same time. In other words using the same `WorkerStates` on
-- `Control.Scheduler.withSchedulerS` concurrently will result in an error. Can be initialized with
-- `Control.Scheduler.initWorkerStates`
--
-- @since 1.4.0
data WorkerStates ws = WorkerStates
  { _workerStatesComp  :: !Comp
  , _workerStatesArray :: !(SmallArray ws)
#if MIN_VERSION_pvar(1,0,0)
  , _workerStatesMutex :: !(PVar Int RealWorld)
#else
  , _workerStatesMutex :: !(PVar IO Int)
#endif
  }

-- | This identifier is needed for tracking batches.
newtype BatchId = BatchId { getBatchId :: Int }
  deriving (Show, Eq, Ord)


-- | Batch is an artifical checkpoint that can be controlled by the user throughout the
-- lifetime of a scheduler.
--
-- @since 1.5.0
data Batch s a = Batch
  { batchCancel      :: a -> ST s Bool
  , batchCancelWith  :: a -> ST s Bool
  , batchHasFinished :: ST s Bool
  }


-- | A thread safe wrapper around `Scheduler`, which allows it to be reused indefinitely
-- and globally if need be. There is one already created in this library:
-- `Control.Scheduler.Global.globalScheduler`
--
-- @since 1.5.0
data GlobalScheduler =
  GlobalScheduler
    { globalSchedulerComp :: !Comp
    , globalSchedulerMVar :: !(MVar (Scheduler RealWorld ()))
    , globalSchedulerThreadIdsRef :: !(IORef [ThreadId])
    }


data SchedulerStatus
  = SchedulerIdle
  | SchedulerWorkerException WorkerException

data TerminateEarlyException =
  TerminateEarlyException
  deriving (Show)

instance Exception TerminateEarlyException

data CancelBatchException =
  CancelBatchException
  deriving (Show)

instance Exception CancelBatchException

-- | This exception should normally be never seen in the wild and is for internal use only.
newtype WorkerException =
  WorkerException SomeException
  -- ^ One of workers experienced an exception, main thread will receive the same `SomeException`.
  deriving (Show)

instance Exception WorkerException

data WorkerTerminateException =
  WorkerTerminateException
  -- ^ When a co-worker dies of some exception, all the other ones will be terminated
  -- asynchronously with this one.
  deriving (Show)


instance Exception WorkerTerminateException

-- | Exception that gets thrown whenever concurrent access is attempted to the `WorkerStates`
--
-- @since 1.4.0
data MutexException =
  MutexException
  deriving (Eq, Show)

instance Exception MutexException where
  displayException MutexException =
    "MutexException: WorkerStates cannot be used at the same time by different schedulers"
