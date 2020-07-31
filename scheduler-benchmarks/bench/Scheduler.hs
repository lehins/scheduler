{-# LANGUAGE BangPatterns #-}

module Main where

import qualified Control.Concurrent.Async as A (mapConcurrently, replicateConcurrently)
import Control.Monad (replicateM_, replicateM)
import Control.Monad.Par (IVar, Par, get, newFull_, runParIO) --, parMapM)
import Control.Parallel (par)
import Control.Scheduler
import Control.Concurrent (getNumCapabilities)
import Control.Concurrent.Async.Pool as AsyncPool
import Criterion.Main
import Control.DeepSeq
import Data.Foldable as F
import Data.IORef
--import Fib
import Streamly (asyncly)
import qualified Streamly.Prelude as S
import UnliftIO.Async (pooledMapConcurrently, pooledReplicateConcurrently)

main :: IO ()
main = do
  let k = 10000
  caps <- getNumCapabilities
  AsyncPool.withTaskGroup caps $ \ !taskGroup ->
    defaultMain $
    [ bgroup
        "withScheduler"
        [ bgroup
            "noop"
            [ bench "Seq" $ whnfIO (withScheduler_ Seq (\_ -> pure ()))
            , bench "Par" $ whnfIO (withScheduler_ Par (\_ -> pure ()))
            , bench "Par'" $ whnfIO (withScheduler_ Par' (\_ -> pure ()))
            ]
        , let schedule s = replicateM_ k $ scheduleWork_ s (pure ())
           in bgroup
                ("pure () - " ++ show k)
                [ bench "trivial" $ whnfIO (schedule trivialScheduler_)
                , bench "Seq" $ whnfIO (withScheduler_ Seq schedule)
                , bench "Par" $ whnfIO (withScheduler_ Par schedule)
                , bench "Par'" $ whnfIO (withScheduler_ Par' schedule)
                ]
        , let schedule s = replicateM_ k $ scheduleWork s (pure ())
           in bgroup
                ("pure [()] - " ++ show k)
                [ bench "trivial" $ whnfIO (withTrivialScheduler schedule)
                , bench "Seq" $ whnfIO (withScheduler Seq schedule)
                , bench "Par" $ whnfIO (withScheduler Par schedule)
                , bench "Par'" $ whnfIO (withScheduler Par' schedule)
                ]
        ]
    , bgroup "libraries" $
      [mkBenchReplicate taskGroup "Sum" n x sumIORef sumParVar | n <- [1000], x <- [1000]] ++
      [mkBenchMap taskGroup "Sum" n sumIO sumParIO sumPar | n <- [2000]]
    ]
  where
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

mkBenchReplicate ::
     NFData a
  => TaskGroup
  -> String
  -> Int -- ^ Number of tasks
  -> Int -- ^ Opaque Int a function should be applied to
  -> (IORef Int -> IO a)
  -> (IVar Int -> Par a)
  -> Benchmark
mkBenchReplicate _taskGroup name n x fxIO fxPar =
  bgroup
    ("replicate/" <> name <> str)
    [ bench "scheduler/replicateConcurrently" $
      nfIO $ replicateConcurrently Par n (newIORef x >>= fxIO)
    , bench "unliftio/pooledReplicateConcurrently" $
      nfIO $ pooledReplicateConcurrently n (newIORef x >>= fxIO)
    , bench "streamly/replicateM" $
      nfIO $ S.drain $ asyncly $ S.replicateM n (newIORef x >>= fxIO)
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
    ("map/" <> name <> str)
    [ bench "scheduler/traverseConcurrently" $ nfIO $ traverseConcurrently Par fxIO [1 .. n]
    , bench "unliftio/pooledMapConcurrently" $ nfIO $ pooledMapConcurrently fxIO [1 .. n]
    , bench "streamly/mapM" $ nfIO $ S.drain $ asyncly $ S.mapM fxIO $ S.enumerateFromTo 1 n
    , bench "async/mapConcurrently" $ nfIO $ A.mapConcurrently fxIO [1 .. n]
    , bench "async-pool/mapConcurrently" $
      nfIO $ AsyncPool.mapConcurrently taskGroup fxIO [1 .. n]
    , bench "par/mapM" $ nfIO $ mapM fxParIO [1 .. n]
    , bench "monad-par/mapM" $ nfIO $ runParIO $ mapM fxPar [1 .. n]
    , bench "base/mapM" $ nfIO $ mapM fxIO [1 .. n]
    ]
  where
    str = "(" ++ show n ++ ")"
