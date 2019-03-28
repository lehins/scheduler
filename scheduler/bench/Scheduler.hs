module Main where

import Control.Monad (replicateM_)
import Control.Concurrent.Async (mapConcurrently, mapConcurrently_)
import Control.Monad.Par (parMapM, runParIO)
import Control.Parallel (par)
import Control.Scheduler
import Criterion.Main
import Data.Foldable as F
import UnliftIO.Async (pooledMapConcurrently, pooledMapConcurrently_)
import Streamly (asyncly)
import qualified Streamly.Prelude as S


main :: IO ()
main = defaultMain (mkSumBench 1000 100000)

mkSumBench :: Int -> Int -> [Benchmark]
mkSumBench n elts =
  [ env (pure ls) $ \xs ->
      bgroup
        ("Sums: " <> show n)
        [ bgroup "unliftio" [bench "pooledMapConcurrently" $ nfIO (pooledMapConcurrently f xs)]
        , bgroup "monad-par" [bench "parMapM" $ nfIO (runParIO $ parMapM f xs)]
        , bgroup "scheduler" [bench "traverseConcurrently" $ nfIO (traverseConcurrently Par f xs)]
        , bgroup "streamly" [bench "mapM" $ nfIO $ S.runStream $ asyncly $ S.mapM f $ S.fromList xs]
        , bgroup "async" [bench "mapConcurrently" $ nfIO (mapConcurrently f xs)]
        , bgroup "base" [bench "traverse . par" $ nfIO (traverse fpar xs)]
        , bgroup "base" [bench "traverse . seq" $ nfIO (traverse f xs)]
        ]
  , env (pure ls) $ \xs ->
      bgroup
        ("Discard Sums: " <> show n)
        [ bgroup "unliftio" [bench "pooledMapConcurrently_" $ nfIO (pooledMapConcurrently_ f xs)]
        , bgroup "scheduler" [bench "traverseConcurrently_" $ nfIO (traverseConcurrently_ Par f xs)]
        , bgroup "async" [bench "mapConcurrently_" $ nfIO (mapConcurrently_ f xs)]
        ]
  , bgroup
      "NoList"
      [ bench "scheduler" $
        nfIO $ withScheduler Par $ \s -> replicateM_ n $ scheduleWork s $ f [0 .. elts]
      , bench "streamly" $
        nfIO $ S.runStream $ asyncly $ S.replicateM n (fstreamly $ S.enumerateFromTo 0 elts)
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
