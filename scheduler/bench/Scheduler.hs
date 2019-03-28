module Main where

import qualified Control.Concurrent.Async as A (mapConcurrently,
                                                mapConcurrently_,
                                                replicateConcurrently,
                                                replicateConcurrently_)
import Control.Monad (replicateM, replicateM_)
import Control.Monad.Par (parMapM, runParIO)
import Control.Parallel (par)
import Control.Scheduler
import Criterion.Main
import Data.Foldable as F
import Streamly (asyncly)
import qualified Streamly.Prelude as S
import UnliftIO.Async (pooledMapConcurrently, pooledMapConcurrently_,
                       pooledReplicateConcurrently,
                       pooledReplicateConcurrently_)


main :: IO ()
main = defaultMain (mkSumBench 1000 100000)

mkSumBench :: Int -> Int -> [Benchmark]
mkSumBench n elts =
  [ -- env (pure elts) $ \x ->
  --     bgroup
  --       ("Replicate Sums (fast): " <> show x)
  --       [ bench "unliftio/pooledReplicateConcurrently" $
  --         nfIO (pooledReplicateConcurrently n $ f [0 .. x])
  --         --replicateConcurrently Par n $ f [0 .. x]
  --       , bench "monad-par/replicateM" $ nfIO $ runParIO $ replicateM n $ f [0 .. x]
  --       ]
  -- , 
    env (pure elts) $ \x ->
      bgroup
        ("Replicate Sums: " <> show x)
        [ bench "scheduler/replicateConcurrently" $
          nfIO $ withScheduler Par $ \s -> replicateM_ n $ scheduleWork s (f [0 .. x])
        , bench "async/replicateConcurrently" $ nfIO $ A.replicateConcurrently n $ f [0 .. x]
        , bench "streamly/replicateM" $
          nfIO $ S.runStream $ asyncly $ S.replicateM n (fstreamly $ S.enumerateFromTo 0 x)
        , bench "base/replicateM" $ nfIO $ replicateM n $ f [0 .. x]
        ]
  -- , bgroup
  --     ("Replicate Discard Sums " <> show n)
  --     [ bench "unliftio/pooledReplicateConcurrently_" $
  --       nfIO (pooledReplicateConcurrently_ n $ f [0 .. elts])
  --     , bench "scheduler/replicateConcurrently_" $
  --       nfIO $ replicateConcurrently_ Par n $ f [0 .. elts]
  --     , bench "monad-par/replicateM_" $ nfIO $ runParIO $ replicateM_ n $ f [0 .. elts]
  --     , bench "async/replicateConcurrently_" $ nfIO $ A.replicateConcurrently_ n $ f [0 .. elts]
  --     , bench "streamly/replicateM" $
  --       nfIO $ S.runStream $ asyncly $ S.replicateM n (fstreamly $ S.enumerateFromTo 0 elts)
  --     ]
  , env (pure ls) $ \xs ->
      bgroup
        ("Sums: " <> show n)
        [ bench "unliftio/pooledMapConcurrently" $ nfIO (pooledMapConcurrently f xs)
        , bench "monad-par/parMapM" $ nfIO (runParIO $ parMapM f xs)
        , bench "scheduler/traverseConcurrently" $ nfIO (traverseConcurrently Par f xs)
        , bench "streamly/mapM" $ nfIO $ S.runStream $ asyncly $ S.mapM f $ S.fromList xs
        , bench "async/mapConcurrently" $ nfIO (A.mapConcurrently f xs)
        , bench "parallel/traverse (par)" $ nfIO (traverse fpar xs)
        , bench "base/traverse (seq)" $ nfIO (traverse f xs)
        ]
  , env (pure ls) $ \xs ->
      bgroup
        ("Discard Traverse Sums: " <> show n)
        [ bgroup "unliftio" [bench "pooledMapConcurrently_" $ nfIO (pooledMapConcurrently_ f xs)]
        , bgroup "scheduler" [bench "traverseConcurrently_" $ nfIO (traverseConcurrently_ Par f xs)]
        , bgroup "async" [bench "mapConcurrently_" $ nfIO (A.mapConcurrently_ f xs)]
        ]
  ]
  where
    ls = replicate n [0 .. elts] :: [[Int]]
    f xs =
      let ys = F.foldl' (+) 0 xs
       in ys `seq` pure ys
    fpar xs =
      let ys = F.foldl' (+) 0 xs
       in ys `par` pure ys
    fstreamly = S.foldl' (+) 0
