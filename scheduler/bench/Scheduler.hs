module Main where

import qualified Control.Concurrent.Async as A (mapConcurrently,
                                                mapConcurrently_,
                                                replicateConcurrently,
                                                replicateConcurrently_)
import Control.Monad (replicateM, replicateM_)
import Control.Monad.Par (IVar, Par, get, newFull_, parMapM, runParIO)
import Control.Parallel (par)
import Control.Scheduler
import Criterion.Main
import Data.Foldable as F
import Data.IORef
import Fib
import Streamly (asyncly)
import qualified Streamly.Prelude as S
import UnliftIO.Async (pooledMapConcurrently, pooledMapConcurrently_,
                       pooledReplicateConcurrently,
                       pooledReplicateConcurrently_)

main :: IO ()
main = do
  defaultMain ([mkFibBench n x | n <- [100], x <- [100]]) --, 1000, 10000], x <- [100, 1000, 10000]])

mkFibBench ::
     Int -- ^ Number of workers
  -> Int -- ^ Fibbonacci number
  -> Benchmark
mkFibBench n x =
  bgroup
    ("Fib " <> show x)
    [ bench ("unliftio/pooledReplicateConcurrently" ++ nStr) $
      nfIO $ pooledReplicateConcurrently n (newIORef x >>= fibM)
    , bench ("scheduler/replicateConcurrently" ++ nStr) $
      nfIO $ replicateConcurrently Par n (newIORef x >>= fibM)
    , bench ("streamly/replicateM" ++ nStr) $
      nfIO $ S.runStream $ asyncly $ S.replicateM n (newIORef x >>= fibM)
    , bench ("monad-par/replicateM" ++ nStr) $
      nfIO $ runParIO $ replicateM n (newFull_ x >>= fibPar)
    , bench ("async/replicateConcurrently" ++ nStr) $
      nfIO $ A.replicateConcurrently n (newIORef x >>= fibM)
    , bench ("base/replicateM" ++ nStr) $ nfIO $ replicateM n (newIORef x >>= fibM)
    ]
  where
    nStr = " (" ++ show n ++ ")"
    fibM :: IORef Int -> IO Integer
    fibM xRef = do
      x' <- readIORef xRef
      let y = fib $ toInteger x'
      y `seq` pure y
    fibPar :: IVar Int -> Par Integer
    fibPar ivar = do
      x' <- get ivar
      let y = fib $ toInteger x'
      y `seq` pure y


-- mkSumBench :: Int -> Int -> [Benchmark]
-- mkSumBench n elts
--     -- env (pure elts) $ \x ->
--   --     bgroup
--   --       ("Replicate Sums (fast): " <> show x)
--   --       [ bench "unliftio/pooledReplicateConcurrently" $
--   --         nfIO (pooledReplicateConcurrently n $ f [0 .. x])
--   --         --replicateConcurrently Par n $ f [0 .. x]
--   --       , bench "monad-par/replicateM" $ nfIO $ runParIO $ replicateM n $ f [0 .. x]
--   --       ]
--   -- ,
--  =
--   [ env (pure f) $ \f' ->
--       bgroup
--         ("Replicate Sums: " <> show n)
--         [ bench "scheduler/replicateConcurrently" $
--           nfIO $ withScheduler_ Par $ \s -> replicateM_ n $ scheduleWork s $ f' [0 .. elts]
--         , bench "scheduler/replicateConcurrently" $
--           nfIO $ replicateConcurrently_ Par n (f' [0 .. elts])
--         , bench "streamly/replicateM" $
--           nfIO $ S.runStream $ asyncly $ S.replicateM n $ f' [0 .. elts]
--         , bench "async/replicateConcurrently" $ nfIO $ A.replicateConcurrently n $ f' [0 .. elts]
--         , bench "base/replicateM" $ nfIO $ replicateM n $ f' [0 .. elts]
--         ]
--   , env (pure 50000) $ \x ->
--       bgroup
--         ("Fib x" <> show n)
--         [ bench "unliftio/pooledReplicateConcurrently" $
--           nfIO $ pooledReplicateConcurrently n (newIORef x >>= fibM)
--         , bench "scheduler/replicateConcurrently" $
--           nfIO $ replicateConcurrently Par n (newIORef x >>= fibM)
--         , bench "monad-par/replicateM" $ nfIO $ runParIO $ replicateM n (newFull_ 1000 >>= fibPar)
--         , bench "streamly/replicateM" $
--           nfIO $ S.runStream $ asyncly $ S.replicateM n (newIORef x >>= fibM)
--         , bench "async/replicateConcurrently" $
--           nfIO $ A.replicateConcurrently n (newIORef x >>= fibM)
--         , bench "base/replicateM" $ nfIO $ replicateM n (newIORef x >>= fibM)
--         ]
--   -- , env (pure fibM) $ \f' ->
--   --     bgroup
--   --       ("Fib: " <> show elts)
--   --       [ bench "scheduler/replicateConcurrently" $ nfIO $ replicateConcurrently Par n $ f' elts
--   --       , bench "streamly/replicateM" $ nfIO $ S.runStream $ asyncly $ S.replicateM n $ f' elts
--   --       , bench "async/replicateConcurrently" $ nfIO $ A.replicateConcurrently n $ f' elts
--   --       , bench "base/replicateM" $ nfIO $ replicateM n $ f' elts
--   --       ]
--   -- , bgroup
--   --     ("Replicate Discard Sums " <> show n)
--   --     [ bench "unliftio/pooledReplicateConcurrently_" $
--   --       nfIO (pooledReplicateConcurrently_ n $ f [0 .. elts])
--   --     , bench "scheduler/replicateConcurrently_" $
--   --       nfIO $ replicateConcurrently_ Par n $ f [0 .. elts]
--   --     , bench "monad-par/replicateM_" $ nfIO $ runParIO $ replicateM_ n $ f [0 .. elts]
--   --     , bench "async/replicateConcurrently_" $ nfIO $ A.replicateConcurrently_ n $ f [0 .. elts]
--   --     , bench "streamly/replicateM" $
--   --       nfIO $ S.runStream $ asyncly $ S.replicateM n (fstreamly $ S.enumerateFromTo 0 elts)
--   --     ]
--   , env (pure ls) $ \xs ->
--       bgroup
--         ("Sums: " <> show n)
--         [ bench "unliftio/pooledMapConcurrently" $ nfIO (pooledMapConcurrently f xs)
--         , bench "monad-par/parMapM" $ nfIO (runParIO $ parMapM f xs)
--         , bench "scheduler/traverseConcurrently" $ nfIO (traverseConcurrently Par f xs)
--         , bench "async/mapConcurrently" $ nfIO (A.mapConcurrently f xs)
--         , bench "parallel/traverse (par)" $ nfIO (traverse fpar xs)
--         , bench "base/traverse (seq)" $ nfIO (traverse f xs)
--         ]
--   , env (pure ls) $ \xs ->
--       bgroup
--         ("Discard Traverse Sums: " <> show n)
--         [ bgroup "unliftio" [bench "pooledMapConcurrently_" $ nfIO (pooledMapConcurrently_ f xs)]
--         , bgroup "scheduler" [bench "traverseConcurrently_" $ nfIO (traverseConcurrently_ Par f xs)]
--         , bgroup "async" [bench "mapConcurrently_" $ nfIO (A.mapConcurrently_ f xs)]
--         ]
--   ]
--   where
--     ls = replicate n [0 .. elts] :: [[Int]]
--     f xs =
--       let ys = F.foldl' (+) 0 xs
--        in ys `seq` pure ys
--     fpar xs =
--       let ys = F.foldl' (+) 0 xs
--        in ys `par` pure ys
