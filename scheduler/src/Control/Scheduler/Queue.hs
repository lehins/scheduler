{-# LANGUAGE NamedFieldPuns #-}
-- |
-- Module      : Control.Scheduler.Queue
-- Copyright   : (c) Alexey Kuleshevich 2018-2019
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Queue
  ( -- * Queue
    -- ** Pure queue
    Queue
  , pushQueue
  , popQueue
  -- ** Job queue
  , Job(Retire, Job_)
  , mkJob
  , JQueue
  , newJQueue
  , pushJQueue
  , popJQueue
  , flushResults
  -- * Tools
  ) where

import Control.Concurrent.MVar
import Control.Monad (join, void)
import Control.Monad.IO.Unlift
import Data.Atomics (atomicModifyIORefCAS)
import Data.IORef


data Queue m a = Queue
  { qQueue   :: ![Job m a]
  , qStack   :: ![Job m a]
  , qResults :: ![IORef a]
  , qBaton   :: !(MVar ())
  }


-- -- | Pure functional Okasaki queue
-- data Queue a = Queue { qQueue :: ![a]
--                      , qStack :: ![a]
--                      }

pushQueue :: Queue m a -> Job m a -> Queue m a
pushQueue queue@Queue {qStack} x = queue {qStack = x : qStack}

popQueue :: Queue m a -> Maybe (Job m a, Queue m a)
popQueue queue@Queue {qQueue, qStack} =
  case qQueue of
    x:xs -> Just (x, queue {qQueue = xs})
    [] ->
      case reverse qStack of
        []   -> Nothing
        y:ys -> Just (y, queue {qQueue = ys, qStack = []})

data Job m a
  = Job !(IORef a) !(m a)
  | Job_ !(m ())
  | Retire


mkJob :: MonadIO m => m a -> m (Job m a)
mkJob action = do
  resRef <- liftIO $ newIORef $ error "mkJob: result is uncomputed"
  return $!
    Job resRef $ do
      res <- action
      liftIO $ writeIORef resRef res
      return res

newtype JQueue m a = JQueue (IORef (Queue m a))

newJQueue :: MonadIO m => m (JQueue m a)
newJQueue = do
  newBaton <- liftIO newEmptyMVar
  queueRef <- liftIO $ newIORef (Queue [] [] [] newBaton)
  return $ JQueue queueRef


pushJQueue :: MonadIO m => JQueue m a -> Job m a -> m ()
pushJQueue (JQueue jQueueRef) job = do
  newBaton <- liftIO newEmptyMVar
  join $
    liftIO $
    atomicModifyIORefCAS
      jQueueRef
      (\(Queue queue stack resRefs baton) ->
         ( Queue
           queue
           (job:stack)
           (case job of
               Job resRef _ -> resRef : resRefs
               _            -> resRefs)
           newBaton
         , liftIO $ putMVar baton ()))


popJQueue :: MonadIO m => JQueue m a -> m (Maybe (m ()))
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
                Job _ action -> return $ Just (void action)
                Job_ action_ -> return $ Just action_
                Retire       -> return Nothing)



flushResults :: MonadIO m => JQueue m a -> m [a]
flushResults (JQueue jQueueRef) =
  liftIO $ do
    resRefs <-
      atomicModifyIORefCAS jQueueRef $ \queue ->
        (queue { qResults = []}, qResults queue)
    mapM readIORef $ reverse resRefs
