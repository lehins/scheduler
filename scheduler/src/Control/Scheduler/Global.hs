-- |
-- Module      : Control.Scheduler.Global
-- Copyright   : (c) Alexey Kuleshevich 2018-2020
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Global
  ( GlobalScheduler
  , newGlobalScheduler
  , waitForBatchGS
  , cancelBatchGS
  , hasBatchFinishedGS
  , scheduleWorkGS
  ) where

import Control.Monad
import Control.Monad.IO.Unlift
import Control.Scheduler
import Control.Scheduler.Internal
import Control.Scheduler.Types
import Data.IORef



newGlobalScheduler :: MonadIO m => Comp -> m GlobalScheduler
newGlobalScheduler comp =
  liftIO $ do
    (jobs, scheduler) <- initScheduler comp scheduleJobs_ (const (pure []))
    ref <- newIORef scheduler
    GlobalScheduler ref <$
      safeBracketOnError
        (spawnWorkers jobs comp)
        terminateWorkers
        (mkWeakIORef ref . terminateWorkers)


waitForBatchGS :: MonadIO m => GlobalScheduler -> m ()
waitForBatchGS (GlobalScheduler ref) = liftIO $ readIORef ref >>= waitForBatch_

cancelBatchGS :: MonadIO m => GlobalScheduler -> m ()
cancelBatchGS (GlobalScheduler ref) =
  liftIO $ readIORef ref >>= \ scheduler -> _cancelCurrentBatch scheduler (Early ())


hasBatchFinishedGS :: MonadIO m => GlobalScheduler -> BatchId -> m Bool
hasBatchFinishedGS (GlobalScheduler ref) batchId =
  liftIO $ readIORef ref >>= \ scheduler -> hasBatchFinished scheduler batchId



scheduleWorkGS :: MonadUnliftIO m => GlobalScheduler -> m a -> m ()
scheduleWorkGS (GlobalScheduler ref) action =
  withRunInIO $ \run -> do
    scheduler <- readIORef ref
    scheduleWork_ scheduler (run (void action))
