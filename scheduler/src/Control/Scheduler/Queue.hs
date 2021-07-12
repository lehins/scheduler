{-# OPTIONS_HADDOCK hide, not-home #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
-- |
-- Module      : Control.Scheduler.Queue
-- Copyright   : (c) Alexey Kuleshevich 2018-2021
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Queue
  (  -- * Job queue
    Job(Job_)
  , mkJob
  , Queue(..)
  , JQueue(..)
  , WorkerId(..)
  , newJQueue
  , pushJQueue
  , popJQueue
  , clearPendingJQueue
  , readResults
  , blockPopJQueue
  , unblockPopJQueue
  ) where

import Primal.Concurrent.MVar
import Primal.Monad
import Data.Maybe
import Primal.Ref

-- | A blocking unbounded queue that keeps the jobs in FIFO order and the results Refs
-- in reversed
data Queue s a = Queue
  { qQueue   :: ![Job s a]
  , qStack   :: ![Job s a]
  , qResults :: ![BRef (Maybe a) s]
  , qBaton   :: {-# UNPACK #-}!(MVar () s)
  }


-- | A unique id for the worker in the `Control.Scheduler.Scheduler` context. It will
-- always be a number from @0@ up to, but not including, the number of workers a scheduler
-- has, which in turn can always be determined with `Control.Scheduler.numWorkers` function.
--
-- @since 1.4.0
newtype WorkerId = WorkerId
  { getWorkerId :: Int
  } deriving (Show, Read, Eq, Ord, Enum, Bounded, Num, Real, Integral)


popQueue :: Queue s a -> Maybe (Job s a, Queue s a)
popQueue queue =
  case qQueue queue of
    x:xs -> Just (x, queue {qQueue = xs})
    [] ->
      case reverse (qStack queue) of
        []   -> Nothing
        y:ys -> Just (y, queue {qQueue = ys, qStack = []})
{-# INLINEABLE popQueue #-}

data Job s a
  = Job {-# UNPACK #-} !(BRef (Maybe a) s) (WorkerId -> ST s ())
  | Job_ (WorkerId -> ST s ())


mkJob :: ((a -> ST s ()) -> WorkerId -> ST s ()) -> ST s (Job s a)
mkJob action = do
  resRef <- newBRef Nothing
  return $ Job resRef (action (writeBRef resRef . Just))
{-# INLINEABLE mkJob #-}

data JQueue s a =
  JQueue
    { jqQueueRef :: {-# UNPACK #-}!(BRef (Queue s a) s)
    , jqLock     :: {-# UNPACK #-}!(MVar () s)
    }

newJQueue :: ST s (JQueue s a)
newJQueue = do
  newLock <- newEmptyMVar
  newBaton <- newEmptyMVar
  queueRef <- newBRef (Queue [] [] [] newBaton)
  return $ JQueue queueRef newLock

-- | Pushes an item onto a queue and returns the previous count.
pushJQueue :: JQueue s a -> Job s a -> ST s ()
pushJQueue (JQueue jQueueRef _) job = do
  newBaton <- newEmptyMVar
  join $
    atomicModifyBRef jQueueRef $ \queue@Queue {qStack, qResults, qBaton} ->
      ( queue
          { qResults =
              case job of
                Job resRef _ -> resRef : qResults
                _            -> qResults
          , qStack = job : qStack
          , qBaton = newBaton
          }
      , putMVar qBaton ())
{-# INLINEABLE pushJQueue #-}

-- | Pops an item from the queue. The job returns the total job counts that is still left
-- in the queue
popJQueue :: JQueue s a -> ST s (WorkerId -> ST s ())
popJQueue (JQueue jQueueRef lock) = inner
  where
    inner = do
      readMVar lock
      join $
        atomicModifyBRef jQueueRef $ \queue@Queue {qBaton} ->
          case popQueue queue of
            Nothing -> (queue, readMVar qBaton >> inner)
            Just (job, newQueue) ->
              ( newQueue
              , case job of
                  Job _ action -> return action
                  Job_ action_ -> return action_)
{-# INLINEABLE popJQueue #-}

unblockPopJQueue :: JQueue s a -> ST s ()
unblockPopJQueue (JQueue _ lock) = putMVar lock ()
{-# INLINEABLE unblockPopJQueue #-}

blockPopJQueue :: JQueue s a -> ST s ()
blockPopJQueue (JQueue _ lock) = takeMVar lock
{-# INLINEABLE blockPopJQueue #-}

-- | Clears any jobs that haven't been started yet. Returns the number of jobs that are
-- still in progress and have not been yet been completed.
clearPendingJQueue :: JQueue s a -> ST s ()
clearPendingJQueue (JQueue queueRef _) =
  atomicModifyBRef_ queueRef $ \queue -> (queue {qQueue = [], qStack = []})
{-# INLINEABLE clearPendingJQueue #-}


-- | Extracts all results available up to now, the uncomputed ones are discarded. This
-- also has an affect of resetting the total job count to zero.
readResults :: JQueue s a -> ST s [a]
readResults (JQueue jQueueRef _) = do
  results <-
    atomicModifyBRef jQueueRef $ \queue ->
      (queue {qQueue = [], qStack = [], qResults = []}, qResults queue)
  catMaybes <$> mapM readBRef results
{-# INLINEABLE readResults #-}
