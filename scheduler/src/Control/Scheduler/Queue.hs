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
    Job(Retire, Job_)
  , mkJob
  , JQueue
  , WorkerId(..)
  , newJQueue
  , pushJQueue
  , popJQueue
  , readResults
  ) where

import Control.Concurrent.MVar
import Control.Monad (join, void)
import Control.Monad.IO.Unlift
import Data.Atomics (atomicModifyIORefCAS)
import Data.IORef


-- | A blocking unbounded queue that keeps the jobs in FIFO order and the results IORefs
-- in reversed
data Queue m a = Queue
  { qQueue   :: ![Job m a]
  , qStack   :: ![Job m a]
  , qResults :: ![IORef (Maybe a)]
  , qBaton   :: !(MVar ())
  }


-- | A unique id for the worker in the `Control.Scheduler.Scheduler` context. It will
-- always be a number from @0@ up to, but not including, the number of workers a scheduler
-- has, which can always be determined by `Control.Scheduler.numWorkers`.
--
-- @since 1.4.0
newtype WorkerId = WorkerId
  { getWorkerId :: Int
  } deriving (Show, Eq, Ord, Enum, Num)


popQueue :: Queue m a -> Maybe (Job m a, Queue m a)
popQueue queue =
  case qQueue queue of
    x:xs -> Just (x, queue {qQueue = xs})
    [] ->
      case reverse (qStack queue) of
        []   -> Nothing
        y:ys -> Just (y, queue {qQueue = ys, qStack = []})

data Job m a
  = Job !(IORef (Maybe a)) (WorkerId -> m a)
  | Job_ (WorkerId -> m ())
  | Retire


mkJob :: MonadIO m => (WorkerId -> m a) -> m (Job m a)
mkJob action = do
  resRef <- liftIO $ newIORef Nothing
  return $!
    Job resRef $ \ i -> do
      res <- action i
      liftIO $ writeIORef resRef $ Just res
      return $! res

newtype JQueue m a = JQueue (IORef (Queue m a))

newJQueue :: MonadIO m => m (JQueue m a)
newJQueue =
  liftIO $ do
    newBaton <- newEmptyMVar
    queueRef <- newIORef (Queue [] [] [] newBaton)
    return $ JQueue queueRef


pushJQueue :: MonadIO m => JQueue m a -> Job m a -> m ()
pushJQueue (JQueue jQueueRef) job =
  liftIO $ do
    newBaton <- newEmptyMVar
    join $
      atomicModifyIORefCAS
        jQueueRef
        (\Queue {qQueue, qStack, qResults, qBaton} ->
           ( Queue
               qQueue
               (job : qStack)
               (case job of
                  Job resRef _ -> resRef : qResults
                  _            -> qResults)
               newBaton
           , liftIO $ putMVar qBaton ()))


popJQueue :: MonadIO m => JQueue m a -> m (Maybe (WorkerId -> m ()))
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
                Job _ action -> return $ Just (void . action)
                Job_ action_ -> return $ Just action_
                Retire       -> return Nothing)



readResults :: MonadIO m => JQueue m a -> m [Maybe a]
readResults (JQueue jQueueRef) =
  liftIO $ do
    resRefs <- qResults <$> readIORef jQueueRef
    mapM readIORef resRefs
