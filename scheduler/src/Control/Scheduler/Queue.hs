{-# OPTIONS_HADDOCK hide, not-home #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- |
-- Module      : Control.Scheduler.Queue
-- Copyright   : (c) Alexey Kuleshevich 2018-2019
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
  , flushJQueue
  , clearPendingJQueue
  , readResults
  ) where

import Control.Concurrent.MVar
import Control.Monad (join, void)
import Control.Monad.IO.Unlift
import Data.Atomics (atomicModifyIORefCAS)
import Data.Maybe
import Data.IORef


-- | A blocking unbounded queue that keeps the jobs in FIFO order and the results IORefs
-- in reversed
data Queue m a = Queue
  { qCount   :: {-# UNPACK #-} !Int
  , qQueue   :: ![Job m a]
  , qStack   :: ![Job m a]
  , qResults :: ![IORef (Maybe a)]
  , qBaton   :: {-# UNPACK #-} !(MVar ())
  }


-- | A unique id for the worker in the `Control.Scheduler.Scheduler` context. It will
-- always be a number from @0@ up to, but not including, the number of workers a scheduler
-- has, which in turn can always be determined with `Control.Scheduler.numWorkers` function.
--
-- @since 1.4.0
newtype WorkerId = WorkerId
  { getWorkerId :: Int
  } deriving (Show, Read, Eq, Ord, Enum, Bounded, Num, Real, Integral)


popQueue :: Queue m a -> Maybe (Job m a, Queue m a)
popQueue queue =
  case qQueue queue of
    x:xs -> Just (x, queue {qCount = qCount queue - 1, qQueue = xs})
    [] ->
      case reverse (qStack queue) of
        []   -> Nothing
        y:ys -> Just (y, queue {qCount = qCount queue - 1, qQueue = ys, qStack = []})

data Job m a
  = Job {-# UNPACK #-} !(IORef (Maybe a)) (WorkerId -> m ())
  | Job_ (WorkerId -> m ())


mkJob :: MonadIO m => ((a -> m ()) -> WorkerId -> m ()) -> m (Job m a)
mkJob action = do
  resRef <- liftIO $ newIORef Nothing
  return $! Job resRef (action (liftIO . writeIORef resRef . Just))

newtype JQueue m a = JQueue (IORef (Queue m a))

newJQueue :: MonadIO m => m (JQueue m a)
newJQueue =
  liftIO $ do
    newBaton <- newEmptyMVar
    queueRef <- newIORef (Queue 0 [] [] [] newBaton)
    return $ JQueue queueRef

-- | Pushes an item onto a queue and returns the previous count.
pushJQueue :: MonadIO m => JQueue m a -> Job m a -> m Int
pushJQueue (JQueue jQueueRef) job =
  liftIO $ do
    newBaton <- newEmptyMVar
    join $
      atomicModifyIORefCAS
        jQueueRef
        (\Queue {qCount, qQueue, qStack, qResults, qBaton} ->
           ( Queue
               (qCount + 1)
               qQueue
               (job : qStack)
               (case job of
                  Job resRef _ -> resRef : qResults
                  _ -> qResults)
               newBaton
           , qCount <$ putMVar qBaton ()))

-- | Pops an item from the queue. Also return current job count (without the popped element)
popJQueue :: MonadIO m => JQueue m a -> m (Int, WorkerId -> m ())
popJQueue (JQueue jQueueRef) = liftIO inner
  where
    inner =
      join $
      atomicModifyIORefCAS jQueueRef $ \queue ->
        case popQueue queue of
          Nothing -> (queue, readMVar (qBaton queue) >> inner)
          Just (job, newQueue) ->
            ( newQueue
            , case job of
                Job _ action -> return (qCount newQueue, action)
                Job_ action_ -> return (qCount newQueue, action_))


-- | Same as `clearPendingJQueue`, but returns the actual that are being removed.
flushJQueue :: MonadIO m => JQueue m a -> m [Job m a]
flushJQueue (JQueue queueRef) =
  liftIO $ atomicModifyIORefCAS queueRef $ \queue ->
    (queue {qCount = 0, qQueue = [], qStack = []}, qQueue queue ++ reverse (qStack queue))

-- | Clears any jobs that haven't been popped yet. Returns the number of jobs that have
-- been removed
clearPendingJQueue :: MonadIO m => JQueue m a -> m Int
clearPendingJQueue (JQueue queueRef) =
  liftIO $ atomicModifyIORefCAS queueRef $ \queue ->
    (queue {qCount = 0, qQueue = [], qStack = []}, length (qQueue queue) + length (qStack queue))


-- | Extracts all results available up to now, the uncomputed ones are discarded.
readResults :: MonadIO m => JQueue m a -> m [a]
readResults (JQueue jQueueRef) =
  liftIO $ do
    results <- atomicModifyIORef' jQueueRef $ \queue -> (queue { qResults = []}, qResults queue)
    rs <- mapM readIORef results
    return $ catMaybes rs
