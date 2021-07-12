{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
-- |
-- Module      : Control.Scheduler.Global
-- Copyright   : (c) Alexey Kuleshevich 2020-2021
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

import Primal.Concurrent (ThreadId)
import Primal.Concurrent.MVar
import Primal.Exception
import Primal.Monad
import Primal.Monad.Unsafe (unsafePerformIO)
import Control.Scheduler
import Control.Scheduler.Internal
import Control.Scheduler.Types
import Data.Maybe
import Primal.Ref

-- | Global scheduler with `Par` computation strategy that can be used anytime using
-- `withGlobalScheduler_`
globalScheduler :: GlobalScheduler
globalScheduler = unsafePerformIO (newGlobalScheduler Par)
{-# NOINLINE globalScheduler #-}


initGlobalScheduler :: Comp -> (Scheduler RW a -> [ThreadId] -> ST RW b) -> ST RW b
initGlobalScheduler comp action = do
  (jobs, mkScheduler) <- initScheduler comp scheduleJobs_ (const (pure []))
  bracketOnError (spawnWorkers jobs comp) terminateWorkers $ \tids ->
    action (mkScheduler tids) tids


-- | Create a new global scheduler, in case a single one `globalScheduler` is not
-- sufficient. It is important to note that too much parallelization can significantly
-- degrate performance, therefore it is best not to use more than one scheduler at a time.
--
-- @since 1.5.0
newGlobalScheduler :: Primal RW m => Comp -> m GlobalScheduler
newGlobalScheduler comp =
  liftST $ initGlobalScheduler comp $ \scheduler tids -> do
    mvar <- newMVar scheduler
    tidsRef <- newBRef tids
    _ <- mkWeakMVar mvar (readBRef tidsRef >>= terminateWorkers)
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
withGlobalScheduler_ :: UnliftPrimal RW m => GlobalScheduler -> (Scheduler RW () -> m a) -> m ()
withGlobalScheduler_ GlobalScheduler {..} action =
  withRunInST $ \run -> do
    let initializeNewScheduler = do
          initGlobalScheduler globalSchedulerComp $ \scheduler tids -> do
            oldTids <- atomicModifyBRef globalSchedulerThreadIdsRef $ (,) tids
            terminateWorkers oldTids
            putMVar globalSchedulerMVar scheduler
    mask $ \restore ->
      tryTakeMVar globalSchedulerMVar >>= \case
        Nothing -> restore $ run $ withScheduler_ globalSchedulerComp action
        Just scheduler -> do
          let runScheduler = do
                  _ <- run $ action scheduler
                  mEarly <- _earlyResults scheduler
                  mEarly <$ when (isNothing mEarly) (void (_waitForCurrentBatch scheduler))
          mEarly <- restore runScheduler `onException` initializeNewScheduler
          -- Whenever a scheduler is terminated it is no longer usable, need to re-initialize
          case mEarly of
            Nothing -> putMVar globalSchedulerMVar scheduler
            Just _  -> initializeNewScheduler
