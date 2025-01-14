{-# LANGUAGE BangPatterns #-}

module Main where

import qualified Control.Concurrent.Async as A (mapConcurrently, replicateConcurrently)
import Control.Monad (replicateM_, replicateM)
import Control.Monad.Par (IVar, Par, get, newFull_, runParIO)
import Control.Monad.ST
import Control.Parallel (par)
import Control.Scheduler
import Control.Scheduler.Global
import Control.Concurrent (getNumCapabilities)
import Control.Concurrent.Async.Pool as AsyncPool
import Control.Concurrent.MVar
import Criterion.Main
import Control.DeepSeq
import Data.Foldable as F
import Data.IORef
import Data.Primitive.PVar
--import Streamly (asyncly)
import qualified Streamly.Data.Fold as SF
import qualified Streamly.Data.Stream.Prelude as S
import UnliftIO.Async (pooledMapConcurrently, pooledReplicateConcurrently, pooledReplicateConcurrently_)



main :: IO ()
main = do
  let k = 10000
  caps <- getNumCapabilities
  AsyncPool.withTaskGroup caps $ \ !taskGroup ->
    defaultMain
      [ bgroup
          "withScheduler"
          [ bgroup
              "noop"
              [ bench "Seq" $ whnfIO (withScheduler_ Seq (\_ -> pure ()))
              , bench "Par" $ whnfIO (withScheduler_ Par (\_ -> pure ()))
              , bench "Par (gloabal)" $
                whnfIO (withGlobalScheduler_ globalScheduler (\_ -> pure ()))
              , bench "Par'" $ whnfIO (withScheduler_ Par' (\_ -> pure ()))
              ]
          , let schedule :: Scheduler RealWorld () -> IO ()
                schedule s = replicateM_ k $ scheduleWork_ s (pure ())
             in bgroup
                  ("pure () - " ++ show k)
                  [ bench "trivial" $ whnfIO (schedule trivialScheduler_)
                  , bench "Seq" $ whnfIO (withScheduler_ Seq schedule)
                  , bench "Par" $ whnfIO (withScheduler_ Par schedule)
                  , bench "Par (gloabal)" $ whnfIO (withGlobalScheduler_ globalScheduler schedule)
                  , bench "Par'" $ whnfIO (withScheduler_ Par' schedule)
                  ]
          , let schedule :: Scheduler RealWorld () -> IO ()
                schedule s = replicateM_ k $ scheduleWork s (pure ())
             in bgroup
                  ("pure [()] - " ++ show k)
                  [ bench "trivial" $ whnfIO (withTrivialScheduler schedule)
                  , bench "Seq" $ whnfIO (withScheduler Seq schedule)
                  , bench "Par" $ whnfIO (withScheduler Par schedule)
                  , bench "Par'" $ whnfIO (withScheduler Par' schedule)
                  ]
          ]
      , bgroup "libraries" $
        [mkBenchIncrement "Inc" n 0 (`atomicAddIntPVar` 1) | n <- [10000]] ++
        [mkBenchMVar "MVar" n 0 incMVar | n <- [10000]] ++
        [mkBenchReplicate "Sum" n x sumIORef sumParVar | n <- [1000], x <- [1000]] ++
        [mkBenchMap taskGroup "Sum" n sumIO sumParIO sumPar | n <- [2000]]
      ]
  where
    incMVar mvar = modifyMVar_ mvar $ \x -> pure $! x + 1
    sumIO :: Int -> IO Int
    sumIO x = do
      let y = F.foldl' (+) 0 [x .. 100 * x]
      y `seq` pure y
    sumParIO :: Int -> IO Int
    sumParIO x = do
      let y = F.foldl' (+) 0 [x .. 100 * x]
      y `par` pure y
    sumPar :: Int -> Par Int
    sumPar x = do
      let y = F.foldl' (+) 0 [x .. 100 * x]
      y `seq` pure y
    sumIORef :: IORef Int -> IO Int
    sumIORef xRef = readIORef xRef >>= sumIO
    sumParVar :: IVar Int -> Par Int
    sumParVar ivar = get ivar >>= sumPar

replicateConcurrentlyGlobal_ :: Int -> IO () -> IO ()
replicateConcurrentlyGlobal_ n f =
  withGlobalScheduler_ globalScheduler $ \s -> replicateM_ n $ scheduleWork_ s f


-- | With lock contention
mkBenchMVar ::
     NFData a
  => String
  -> Int -- ^ Number of parallel tasks
  -> Int -- ^ Initial value
  -> (MVar Int -> IO a)
  -> Benchmark
mkBenchMVar name n x fxIO =
  bgroup
    ("increment/" ++ name ++ str)
    [ bench "scheduler/replicateConcurrently" $
      nfIO $ replicateConcurrently Par n (newMVar x >>= fxIO)
    , bench "scheduler/replicateConcurrently_" $
      nfIO $ replicateConcurrently_ Par n (newMVar x >>= fxIO >>= \ !_ -> pure ())
    , bench "scheduler/replicateConcurrently_ (global)" $
      nfIO $ replicateConcurrentlyGlobal_ n (newMVar x >>= fxIO >>= \ !_ -> pure ())
    , bench "unliftio/pooledReplicateConcurrently" $
      nfIO $ pooledReplicateConcurrently_ n (newMVar x >>= fxIO)
    , bench "streamly/replicateM" $
      nfIO $ S.fold SF.drain $ S.parEval id $ S.replicateM n (newMVar x >>= fxIO)
    , bench "async/replicateConcurrently" $ nfIO $ A.replicateConcurrently n (newMVar x >>= fxIO)
    , bench "base/replicateM" $ nfIO $ replicateM n (newMVar x >>= fxIO)
    ]
  where
    str = "(" ++ show n ++ ")"

-- | Lock-free contention
mkBenchIncrement ::
     NFData a
  => String
  -> Int -- ^ Number of parallel tasks
  -> Int -- ^ Initial value
  -> (PVar Int RealWorld -> IO a)
  -> Benchmark
mkBenchIncrement name n x fxIO =
  bgroup
    ("increment/" ++ name ++ str)
    [ bench "scheduler/replicateConcurrently" $
      nfIO $ replicateConcurrently Par n (newPVar x >>= fxIO)
    , bench "scheduler/replicateConcurrently_" $
      nfIO $ replicateConcurrently_ Par n (newPVar x >>= fxIO >>= \ !_ -> pure ())
    , bench "scheduler/replicateConcurrently_ (global)" $
      nfIO $ replicateConcurrentlyGlobal_ n (newPVar x >>= fxIO >>= \ !_ -> pure ())
    , bench "unliftio/pooledReplicateConcurrently" $
      nfIO $ pooledReplicateConcurrently_ n (newPVar x >>= fxIO)
    , bench "streamly/replicateM" $
      nfIO $ S.fold SF.drain $ S.parEval id $ S.replicateM n (newPVar x >>= fxIO)
    , bench "async/replicateConcurrently" $ nfIO $ A.replicateConcurrently n (newPVar x >>= fxIO)
    , bench "base/replicateM" $ nfIO $ replicateM n (newPVar x >>= fxIO)
    ]
  where
    str = "(" ++ show n ++ ")"

-- | No contention
mkBenchReplicate ::
     NFData a
  => String
  -> Int -- ^ Number of tasks
  -> Int -- ^ Opaque Int that function will be applied to
  -> (IORef Int -> IO a)
  -> (IVar Int -> Par a)
  -> Benchmark
mkBenchReplicate name n x fxIO fxPar =
  bgroup
    ("replicate/" ++ name ++ str)
    [ bench "scheduler/replicateConcurrently" $
      nfIO $ replicateConcurrently Par n (newIORef x >>= fxIO)
    , bench "scheduler/replicateConcurrently_" $
      nfIO $ replicateConcurrently_ Par n (newIORef x >>= fxIO >>= \ !_ -> pure ())
    , bench "scheduler/replicateConcurrently_ (global)" $
      nfIO $ replicateConcurrentlyGlobal_ n (newIORef x >>= fxIO >>= \ !_ -> pure ())
    , bench "unliftio/pooledReplicateConcurrently" $
      nfIO $ pooledReplicateConcurrently n (newIORef x >>= fxIO)
    , bench "unliftio/pooledReplicateConcurrently_" $
      nfIO $ pooledReplicateConcurrently_ n (newIORef x >>= fxIO)
    , bench "streamly/replicateM" $
      nfIO $ S.fold SF.drain $ S.parEval id $ S.replicateM n (newIORef x >>= fxIO)
    , bench "async/replicateConcurrently" $ nfIO $ A.replicateConcurrently n (newIORef x >>= fxIO)
    , bench "monad-par/replicateM" $ nfIO $ runParIO $ replicateM n (newFull_ x >>= fxPar)
    , bench "base/replicateM" $ nfIO $ replicateM n (newIORef x >>= fxIO)
    ]
  where
    str = "(" ++ show n ++ "/" ++ show x ++ ")"


mkBenchMap ::
     NFData a
  => TaskGroup
  -> String
  -> Int -- ^ Number of tasks
  -> (Int -> IO a)
  -> (Int -> IO a)
  -> (Int -> Par a)
  -> Benchmark
mkBenchMap taskGroup name n fxIO fxParIO fxPar =
  bgroup
    ("map/" ++ name ++ str)
    [ bench "scheduler/traverseConcurrently" $ nfIO $ traverseConcurrently Par fxIO [1 .. n]
    , bench "unliftio/pooledMapConcurrently" $ nfIO $ pooledMapConcurrently fxIO [1 .. n]
    , bench "streamly/mapM" $ nfIO $ S.fold SF.drain $ S.parEval id $ S.mapM fxIO $ S.enumerateFromTo 1 n
    , bench "async/mapConcurrently" $ nfIO $ A.mapConcurrently fxIO [1 .. n]
    -- , bench "async-pool/mapConcurrently" $
    --   nfIO $ AsyncPool.mapConcurrently taskGroup fxIO [1 .. n]
    , bench "par/mapM" $ nfIO $ mapM fxParIO [1 .. n]
    , bench "monad-par/mapM" $ nfIO $ runParIO $ mapM fxPar [1 .. n]
    , bench "base/mapM" $ nfIO $ mapM fxIO [1 .. n]
    ]
  where
    str = "(" ++ show n ++ ")"
