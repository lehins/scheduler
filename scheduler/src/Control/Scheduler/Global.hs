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
  , waitForGS
  , scheduleWorkGS
  ) where

import Control.Monad
import Control.Monad.IO.Unlift
import Control.Scheduler
import Control.Scheduler.Internal
import Control.Scheduler.Types
import Data.IORef



newGlobalScheduler :: MonadIO m => m GlobalScheduler
newGlobalScheduler =
  liftIO $ do
    (jobs, scheduler) <- initScheduler Par scheduleJobs_ (const (pure []))
    ref <- newIORef scheduler
    GlobalScheduler ref <$
      safeBracketOnError
        (spawnWorkers jobs Par)
        terminateWorkers
        (mkWeakIORef ref . terminateWorkers)

waitForGS :: MonadIO m => GlobalScheduler -> m ()
waitForGS (GlobalScheduler ref) = liftIO $ readIORef ref >>= waitForResults_

scheduleWorkGS :: MonadUnliftIO m => GlobalScheduler -> m a -> m ()
scheduleWorkGS (GlobalScheduler ref) action =
  withRunInIO $ \run -> do
    scheduler <- readIORef ref
    scheduleWork_ scheduler (run (void action))
