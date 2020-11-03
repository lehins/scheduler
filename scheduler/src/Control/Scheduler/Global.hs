{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
-- |
-- Module      : Control.Scheduler.Global
-- Copyright   : (c) Alexey Kuleshevich 2020
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Global
  ( GlobalScheduler
  , globalScheduler
  , newGlobalScheduler
  , withGlobalScheduler_
  ) where

import Data.Maybe
import Control.Concurrent (ThreadId)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Control.Monad.IO.Unlift
import Control.Scheduler
import Control.Scheduler.Internal
import Control.Scheduler.Types
import Data.IORef
import System.IO.Unsafe (unsafePerformIO)

-- | Global scheduler with `Par` computation strategy that can be used anytime using
-- `withGlobalScheduler_`
globalScheduler :: GlobalScheduler IO
globalScheduler = unsafePerformIO (newGlobalScheduler Par)
{-# NOINLINE globalScheduler #-}


initGlobalScheduler ::
     MonadUnliftIO m => Comp -> (Scheduler m a -> [ThreadId] -> m b) -> m b
initGlobalScheduler comp action = do
  (jobs, mkScheduler) <- initScheduler comp scheduleJobs_ (const (pure []))
  safeBracketOnError (spawnWorkers jobs comp) (liftIO . terminateWorkers) $ \tids ->
    action (mkScheduler tids) tids


-- | Create a new global scheduler, in case a single one `globalScheduler` is not
-- sufficient. It is important to note that too much parallelization can significantly
-- degrate performance, therefore it is best not to use more than one scheduler at a time.
--
-- @since 1.5.0
newGlobalScheduler :: MonadUnliftIO m => Comp -> m (GlobalScheduler m)
newGlobalScheduler comp =
  initGlobalScheduler comp $ \scheduler tids ->
    liftIO $ do
      mvar <- newMVar scheduler
      tidsRef <- newIORef tids
      _ <- mkWeakMVar mvar (readIORef tidsRef >>= terminateWorkers)
      pure $
        GlobalScheduler
          { globalSchedulerComp = comp
          , globalSchedulerMVar = mvar
          , globalSchedulerThreadIdsRef = tidsRef
          }

-- | Use the global scheduler if it is not busy, otherwise initialize a temporary one. It
-- means that this function by itself will not block, but if the same global scheduler
-- used concurrently other schedulers might get created.
--
-- @since 1.5.0
withGlobalScheduler_ :: MonadUnliftIO m => GlobalScheduler m -> (Scheduler m () -> m a) -> m ()
withGlobalScheduler_ GlobalScheduler {..} action =
  withRunInIO $ \run -> do
    let initializeNewScheduler = do
          initGlobalScheduler globalSchedulerComp $ \scheduler tids ->
            liftIO $ do
              oldTids <- atomicModifyIORef' globalSchedulerThreadIdsRef $ (,) tids
              terminateWorkers oldTids
              putMVar globalSchedulerMVar scheduler
    mask $ \restore ->
      tryTakeMVar globalSchedulerMVar >>= \case
        Nothing -> restore $ run $ withScheduler_ globalSchedulerComp action
        Just scheduler -> do
          let runScheduler =
                run $ do
                  _ <- action scheduler
                  mEarly <- _earlyResults scheduler
                  mEarly <$ when (isNothing mEarly) (void (_waitForCurrentBatch scheduler))
          mEarly <- restore runScheduler `onException` run initializeNewScheduler
          -- Whenever a scheduler is terminated it is no longer usable, need to re-initialize
          case mEarly of
            Nothing -> putMVar globalSchedulerMVar scheduler
            Just _ -> run initializeNewScheduler
