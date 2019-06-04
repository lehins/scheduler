{-# OPTIONS_HADDOCK hide, not-home #-}
{-# LANGUAGE Unsafe #-}
-- |
-- Module      : Control.Scheduler.Internal
-- Copyright   : (c) Alexey Kuleshevich 2018-2019
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Internal
  ( Scheduler(..)
  , WorkerStates(..)
  , SchedulerWS(..)
  , Jobs(..)
  , SchedulerOutcome(..)
  , WorkerException(..)
  , WorkerTerminateException(..)
  , MutexException(..)
  ) where

import Control.Exception
import Control.Scheduler.Computation
import Control.Scheduler.Queue
import Data.IORef
import Data.Primitive.Array


data Jobs m a = Jobs
  { jobsNumWorkers :: {-# UNPACK #-} !Int
  , jobsQueue      :: !(JQueue m a)
  , jobsCountRef   :: !(IORef Int)
  }

-- | Main type for scheduling work. See `Control.Scheduler.withScheduler` or
-- `Control.Scheduler.withScheduler_` for ways to construct and use this data type.
--
-- @since 1.0.0
data Scheduler m a = Scheduler
  { _numWorkers     :: {-# UNPACK #-} !Int
  , _scheduleWorkId :: (WorkerId -> m a) -> m ()
  , _terminate      :: a -> m a
  , _terminateWith  :: a -> m a
  }

-- | This is a wrapper around `Scheduler`, but it also keeps a separate state for each
-- individual worker. See `Control.Scheduler.withSchedulerWS` or
-- `Control.Scheduler.withSchedulerWS_` for ways to construct and use this data type.
--
-- @since 1.4.0
data SchedulerWS s m a = SchedulerWS
  { _workerStates :: !(WorkerStates s)
  , _getScheduler :: !(Scheduler m a)
  }

-- | Each worker is capable of keeping it's own state, that can be share for different
-- schedulers, but not at the same time. In other words using the same `WorkerStates` on
-- `Control.Scheduler.withSchedulerS` concurrently will result in an error. Can be initialized with
-- `Control.Scheduler.initWorkerStates`
--
-- @since 1.4.0
data WorkerStates s = WorkerStates
  { _workerStatesComp  :: !Comp
  , _workerStatesArray :: !(Array s)
  , _workerStatesMutex :: !(IORef Bool)
  }


data SchedulerOutcome a
  = SchedulerFinished
  | SchedulerTerminatedEarly ![a]
  | SchedulerWorkerException WorkerException


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

-- | Exception that gets thrown whenever concurrent access is attempted to the `WorkerStates`
--
-- @since 1.4.0
data MutexException =
  MutexException
  deriving (Eq, Show)

instance Exception MutexException where
  displayException MutexException =
    "MutexException: WorkerStates cannot be used at the same time by different schedulers"
