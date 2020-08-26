-- |
-- Module      : Control.Scheduler.Global
-- Copyright   : (c) Alexey Kuleshevich 2018-2020
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Scheduler.Global
  ( -- * This module is still experimental and the API is likely to change.
    GlobalScheduler
  , newGlobalScheduler
  , waitForBatchGS
  , cancelBatchGS
  , getCurrentBatchIdGS
  , hasBatchFinishedGS
  , scheduleWorkGS
  ) where

import Control.Exception
import Control.Monad
import Control.Monad.IO.Unlift
import Control.Scheduler
import Control.Scheduler.Internal
import Control.Scheduler.Types
import Data.IORef



newGlobalScheduler :: MonadIO m => Comp -> m GlobalScheduler
newGlobalScheduler comp =
  liftIO $ do
    (jobs, mkScheduler) <- initScheduler comp scheduleJobs_ (const (pure []))
    safeBracketOnError (spawnWorkers jobs comp) terminateWorkers $ \tids -> do
      ref <- newIORef $ mkScheduler tids
      GlobalScheduler ref <$ mkWeakIORef ref (terminateWorkers tids)



waitForBatchGS :: MonadIO m => GlobalScheduler -> m ()
waitForBatchGS (GlobalScheduler ref) = liftIO $ readIORef ref >>= waitForBatch_

cancelBatchGS :: MonadIO m => GlobalScheduler -> m ()
cancelBatchGS (GlobalScheduler ref) =
  liftIO $ readIORef ref >>= \ scheduler -> _cancelCurrentBatch scheduler (Early ())


getCurrentBatchIdGS :: MonadIO m => GlobalScheduler -> m BatchId
getCurrentBatchIdGS (GlobalScheduler ref) = liftIO $ readIORef ref >>= _currentBatchId

hasBatchFinishedGS :: MonadIO m => GlobalScheduler -> BatchId -> m Bool
hasBatchFinishedGS (GlobalScheduler ref) batchId =
  liftIO $ readIORef ref >>= \ scheduler -> hasBatchFinished scheduler batchId



scheduleWorkGS :: MonadUnliftIO m => GlobalScheduler -> m a -> m ()
scheduleWorkGS (GlobalScheduler ref) action =
  withRunInIO $ \run -> do
    scheduler <- readIORef ref
    scheduleWork_ scheduler (run (void action))



safeBracketOnError :: MonadUnliftIO m => m a -> (a -> m b) -> (a -> m c) -> m c
safeBracketOnError before after thing =
  withRunInIO $ \run ->
    mask $ \restore -> do
      x <- run before
      res1 <- try $ restore $ run $ thing x
      case res1 of
        Left (e1 :: SomeException) -> do
          _ :: Either SomeException b <- try $ uninterruptibleMask_ $ run $ after x
          throwIO e1
        Right y -> return y
