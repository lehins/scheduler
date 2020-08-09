{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Control.SchedulerSpec
  ( spec
  ) where

import Data.Int
import Control.Concurrent (killThread, myThreadId, threadDelay, yield)
import Control.Concurrent.MVar
import Control.DeepSeq
import qualified Control.Exception as EUnsafe
import Control.Exception.Base (ArithException(DivideByZero),
                               AsyncException(ThreadKilled))
import Control.Monad
import Control.Scheduler as S
import Data.Bits (complement)
import qualified Data.Foldable as F (toList, traverse_)
import Data.IORef
import Data.List (groupBy, sort, sortOn)
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Test.QuickCheck.Function
import Test.QuickCheck.Monadic
import Test.Validity.Eq
import Test.Validity.Functor
import Test.Validity.Monoid
import Test.Validity.Ord
import Test.Validity.Show
import UnliftIO.Async
import UnliftIO.Exception hiding (assert)
#if !MIN_VERSION_base(4,11,0)
import Data.Semigroup
#endif


concurrentProperty :: Testable prop => prop -> Property
concurrentProperty = within 2000000

concurrentExpectation :: Expectation -> Property
concurrentExpectation = concurrentProperty

concurrentPropertyIO :: IO Property -> Property
concurrentPropertyIO = concurrentProperty . monadicIO . run

instance Arbitrary Comp where
  arbitrary = frequency [(20, pure Seq), (80, getNonSeq <$> arbitrary)]

newtype NonSeq = NonSeq {getNonSeq :: Comp }
  deriving (Show, Eq)

instance Arbitrary NonSeq where
  arbitrary =
    NonSeq <$>
    frequency [(10, pure Par), (35, ParOn <$> arbitrary), (35, ParN . getSmall <$> arbitrary)]

prop_SameList :: Comp -> [Int] -> Property
prop_SameList comp xs =
  concurrentPropertyIO $ do
    xs' <- withScheduler comp $ \scheduler -> mapM_ (scheduleWork scheduler . return) xs
    return (xs === xs')

prop_Recursive :: Comp -> [Int] -> Property
prop_Recursive comp xs =
  concurrentPropertyIO $ do
    xs' <- withScheduler comp (schedule xs)
    return (sort xs === sort xs')
  where
    schedule [] _ = return ()
    schedule (y:ys) scheduler = scheduleWork scheduler (schedule ys scheduler >> return y)


prop_Serially :: Comp -> [Int] -> Property
prop_Serially comp xs =
  concurrentPropertyIO $ do
    xs' <- schedule xs
    return (xs === concat xs')
  where
    schedule [] = return []
    schedule (y:ys) = do
      y' <- withScheduler comp (`scheduleWork` pure y)
      ys' <- schedule ys
      return (y':ys')

prop_Nested :: Comp -> [Int] -> Property
prop_Nested comp xs =
  concurrentPropertyIO $ do
    xs' <- schedule xs
    return (sort xs === sort (concat xs'))
  where
    schedule [] = return []
    schedule (y:ys) =
      withScheduler comp (\s -> scheduleWork s (schedule ys >>= \ys' -> return (y : concat ys')))

-- | Check whether all jobs have been completed (similar roprop_Traverse)
prop_AllJobsProcessed :: Comp -> [Int] -> Property
prop_AllJobsProcessed comp jobs =
  concurrentProperty $
  monadicIO
    ((=== jobs) <$>
     run (withScheduler comp $ \scheduler -> mapM_ (scheduleWork scheduler . pure) jobs))

prop_Traverse :: Comp -> [Int] -> Fun Int Int -> Property
prop_Traverse comp xs f =
  concurrentPropertyIO $ (===) <$> traverse f' xs <*> traverseConcurrently comp f' xs
  where
    f' = pure . apply f

replicateSeq :: (Int -> IO Int -> IO [Int]) -> Int -> Fun Int Int -> Property
replicateSeq justAs n f =
  concurrentPropertyIO $ do
    iRef <- newIORef 0
    jRef <- newIORef 0
    let g ref = atomicModifyIORef' ref (\i -> (apply f i, i + 1))
    (===) <$> S.replicateConcurrently Seq n (g jRef) <*> justAs n (g iRef)

prop_ReplicateM :: Int -> Fun Int Int -> Property
prop_ReplicateM i = concurrentProperty . replicateSeq replicateM i

prop_ReplicateWorkSeq :: Int -> Fun Int Int -> Property
prop_ReplicateWorkSeq i =
  concurrentProperty . replicateSeq (\ n g -> withScheduler Seq (\s -> replicateWork n s g)) i


prop_ManyJobsInChunks :: Comp -> [[Int]] -> Property
prop_ManyJobsInChunks comp jss =
  concurrentExpectation $ do
    xs <- withScheduler comp $ \s ->
      forM_ jss $ \js -> do
        mapM_ (scheduleWork s . pure) js
        rs <- waitForBatch s
        rs `shouldBe` js
    xs `shouldBe` []

prop_ArbitraryCompNested :: [(Comp, Int)] -> Property
prop_ArbitraryCompNested xs =
  concurrentPropertyIO $ do
    xs' <- schedule xs
    return (sort (map snd xs) === sort (concat xs'))
  where
    schedule [] = return []
    schedule ((c, y):ys) =
      withScheduler c (\s -> scheduleWork s (schedule ys >>= \ys' -> return (y : concat ys')))

-- | Ensure proper exception handling.
prop_CatchDivideByZero :: Comp -> Int -> [Positive Int] -> Property
prop_CatchDivideByZero comp k xs =
  concurrentProperty $
  assertExceptionIO
    (== DivideByZero)
    (traverseConcurrently
       comp
       (\i -> return (k `div` i))
       (map getPositive xs ++ [0] ++ map getPositive xs))

-- | Ensure proper exception handling.
prop_CatchDivideByZeroNested :: Comp -> Int -> Positive Int -> Property
prop_CatchDivideByZeroNested comp a (Positive k) =
  concurrentProperty $ assertExceptionIO (== DivideByZero) (schedule k)
  where
    schedule i
      | i < 0 = return []
      | otherwise =
        withScheduler comp (\s -> scheduleWork s (schedule (i - 1) >> return (a `div` i)))


-- | Make sure one co-worker can kill another one, of course when there are at least two of.
prop_KillBlockedCoworker :: Comp -> Property
prop_KillBlockedCoworker comp =
  concurrentProperty $
  assertExceptionIO
    (== DivideByZero)
    (withScheduler_ comp $ \scheduler ->
       if numWorkers scheduler < 2
         then scheduleWork scheduler $ return ((1 :: Int) `div` (0 :: Int))
         else do
           mv <- newEmptyMVar
           scheduleWork scheduler $ readMVar mv
           scheduleWork scheduler $ return ((1 :: Int) `div` (0 :: Int)))

-- | Make sure one co-worker can kill another one, of course when there are at least two of.
prop_KillSleepingCoworker :: Comp -> Property
prop_KillSleepingCoworker comp =
  concurrentProperty $
  assertExceptionIO
    (== DivideByZero)
    (withScheduler_ comp $ \scheduler -> do
       scheduleWork scheduler $ return ((1 :: Int) `div` (0 :: Int))
       scheduleWork scheduler $ do
         threadDelay 500000
         error "This should never happen! Thread should have been killed by now.")


prop_ExpectAsyncException :: Comp -> Property
prop_ExpectAsyncException comp =
  concurrentProperty $
  let didAWorkerDie =
        EUnsafe.handleJust EUnsafe.asyncExceptionFromException (return . (== EUnsafe.ThreadKilled)) .
        fmap or
   in (monadicIO . run . didAWorkerDie . withScheduler comp $ \s ->
         scheduleWork s (myThreadId >>= killThread >> pure False)) .&&.
      (monadicIO . run . fmap not . didAWorkerDie . withScheduler Par $ \s ->
         scheduleWork s $ pure False)

prop_WorkerCaughtAsyncException :: Positive Int -> Property
prop_WorkerCaughtAsyncException (Positive n) =
  concurrentProperty $
  assertExceptionIO (== DivideByZero) $ do
    lock <- newEmptyMVar
    result <-
      race (readMVar lock) $
      withScheduler_ (ParN 2) $ \scheduler -> do
        scheduleWork scheduler $ do
          threadDelay (n `mod` 1000000)
          EUnsafe.throwIO DivideByZero
        scheduleWork scheduler $ do
          e <- tryAny $ replicateM_ 5 $ threadDelay 1000000
          case e of
            Right _ -> throwString "Impossible, shouldn't have waited for so long"
            Left exc -> do
              putMVar lock exc
              throwString $
                "I should not have survived: " ++ displayException (exc :: SomeException)
    void $ throwString $
      case result of
        Left innerError -> "Scheduled job cought async exception: " ++ displayException innerError
        Right () -> "Scheduler terminated properly. Should not have happened"

-- | Make sure there is no problems if sub-schedules worker get killed
prop_AllWorkersDied :: Comp -> Comp -> Positive Int -> Property
prop_AllWorkersDied comp1 comp (Positive n) =
  concurrentProperty $
  assertAsyncExceptionIO
    (== ThreadKilled)
    (withScheduler_ comp1 $ \scheduler1 ->
       scheduleWork
         scheduler1
         (withScheduler_ comp $ \scheduler ->
            replicateM_ n (scheduleWork scheduler (myThreadId >>= killThread))))

prop_FinishEarly_ :: NonSeq -> Property
prop_FinishEarly_ (NonSeq comp) =
  concurrentPropertyIO $ do
    ref <- newIORef True
    withScheduler_ comp $ \scheduler ->
      scheduleWork_
        scheduler
        (terminate_ scheduler >> yield >> threadDelay 10000 >> writeIORef ref False)
    counterexample "Scheduler did not terminate early" <$> readIORef ref

prop_FinishEarly :: Comp -> Property
prop_FinishEarly comp =
  concurrentPropertyIO $ do
    let scheduleJobs scheduler = do
          scheduleWork scheduler (pure (2 :: Int))
          scheduleWork scheduler (threadDelay 10000 >> terminate scheduler 3 >> pure 1)
    res <- withScheduler comp scheduleJobs
    res' <- withSchedulerR comp scheduleJobs
    pure (res === [2, 3] .&&. res' === FinishedEarly [2] 3)

prop_FinishEarlyWith :: Comp -> Int -> Property
prop_FinishEarlyWith comp n =
  concurrentPropertyIO $ do
    let scheduleJobs scheduler = do
          scheduleWork scheduler $ pure (complement (n + 1))
          scheduleWork scheduler $ terminateWith scheduler n >> pure (complement n)
    res <- withScheduler comp scheduleJobs
    res' <- withSchedulerR comp scheduleJobs
    pure (res === [n] .&&. res' === FinishedEarlyWith n)

prop_FinishBeforeStarting :: Comp -> Property
prop_FinishBeforeStarting comp =
  concurrentPropertyIO $ do
    res <-
      withScheduler comp $ \scheduler -> do
        void $ terminate scheduler 1
        scheduleWork scheduler (threadDelay 10000 >> pure 2)
    pure (res === [1 :: Int])

prop_FinishWithBeforeStarting :: Comp -> Int -> Property
prop_FinishWithBeforeStarting comp n =
  concurrentPropertyIO $ do
    res <-
      withScheduler comp $ \scheduler -> do
        void $ terminateWith scheduler n
        scheduleWork scheduler $ pure (complement n)
    pure (res === [n])

prop_TrivialSchedulerSameAsSeq_ :: [Int] -> Property
prop_TrivialSchedulerSameAsSeq_ zs =
  concurrentPropertyIO $ do
    let consRef xsRef x = atomicModifyIORef' xsRef $ \ xs -> (x:xs, ())
        trivial = trivialScheduler_
    nRef <- newIORef False
    xRefs <- newIORef []
    yRefs <- newIORef []
    withScheduler_ Seq $ \scheduler -> do
      writeIORef nRef (numWorkers scheduler == numWorkers trivial)
      mapM_ (scheduleWork_ scheduler . consRef xRefs) zs
    mapM_ (scheduleWork_ trivial . consRef yRefs) zs
    nSame <- readIORef nRef
    xs <- readIORef xRefs
    ys <- readIORef yRefs
    pure (nSame .&&. xs === ys)

prop_SameAsTrivialScheduler :: Comp -> [Int] -> Fun Int Int -> Property
prop_SameAsTrivialScheduler comp zs f =
  concurrentPropertyIO $ do
    let schedule scheduler = forM_ zs (scheduleWork scheduler . pure . apply f)
    xs <- withScheduler comp schedule
    ys <- withTrivialScheduler schedule
    pure (xs === ys)

prop_Terminate ::
     (Show a, Eq a)
  => ((Scheduler IO Int -> IO ()) -> IO a)
  -> (Scheduler IO Int -> Int -> IO Int)
  -> ([Int] -> Int -> a)
  -> [Int]
  -> Int
  -> [Int]
  -> Property
prop_Terminate withSchedulerR' term expected xs x ys =
  concurrentExpectation $ do
    rs <-
      withSchedulerR' $ \scheduler -> do
        forM_ xs (scheduleWork scheduler . pure)
        _ <- scheduleWork scheduler $ term scheduler x
        forM_ ys (scheduleWork scheduler . pure)
    rs `shouldBe` expected xs x

-- prop_TerminateSeq ::
--      ((Scheduler IO Int -> IO ()) -> IO (Results Int)) -> [Int] -> Int -> [Int] -> Expectation
-- prop_TerminateSeq withSchedulerR' xs x ys = do
--   rs <- withSchedulerR' $ \ scheduler -> do
--     forM_ xs (scheduleWork scheduler . pure)
--     _ <- scheduleWork scheduler $ terminate scheduler x
--     forM_ ys (scheduleWork scheduler . pure)
--   rs `shouldBe` FinishedEarly xs x

-- prop_TerminateWithSeq ::
--      ((Scheduler IO Int -> IO ()) -> IO (Results Int)) -> [Int] -> Int -> [Int] -> Expectation
-- prop_TerminateWithSeq withSchedulerR' xs x ys = do
--   rs <- withSchedulerR' $ \ scheduler -> do
--     forM_ xs (scheduleWork scheduler . pure)
--     _ <- scheduleWork scheduler $ terminateWith scheduler x
--     forM_ ys (scheduleWork scheduler . pure)
--   rs `shouldBe` FinishedEarlyWith x


newtype Elem = Elem Int deriving (Eq, Show)

instance Exception Elem


-- | Check if an element is in the list with an exception
prop_TraverseConcurrently_ :: Comp -> [Int] -> Int -> Property
prop_TraverseConcurrently_ comp xs x =
  concurrentPropertyIO $ do
    let f i
          | i == x = throwIO $ Elem x
          | otherwise = pure ()
    eRes :: Either Elem () <- try $ traverse_ f xs
    eRes' <- try $ traverseConcurrently_ comp f xs
    return (eRes === eRes')

-- TODO: fix the infinite property for single worker schedulers
-- | Check if an element is in the list with an exception, where we know that list is infinite and
-- element is part of that list.
prop_TraverseConcurrentlyInfinite_ :: NonSeq -> [Int] -> Int -> Property
prop_TraverseConcurrentlyInfinite_ (NonSeq comp) xs x =
  concurrentPropertyIO $ do
    let f i
          | i == x = throwIO $ Elem x
          | otherwise = pure ()
        xs' = xs ++ [x] -- ++ [0 ..]
    eRes :: Either Elem () <- try $ F.traverse_ f xs'
    eRes' <- try $ traverseConcurrently_ comp f xs'
    return (eRes === eRes')


prop_WorkerStateExclusive :: Comp -> NonNegative Int -> Property
prop_WorkerStateExclusive comp (NonNegative n) =
  concurrentExpectation $ do
    state <- initWorkerStates comp (\wid -> (,) wid <$> newIORef (0 :: Int))
    workerStatesComp state `shouldBe` comp
    nWorkers <- getCompWorkers comp
    let scheduleJobs schedulerWS = do
          replicateM n $
            scheduleWorkState schedulerWS $ \(wid, ref) -> do
              counter <- readIORef ref
              writeIORef ref (counter + 1)
              pure (wid, counter)
        gather = map (sortOn snd) . groupBy (\x y -> fst x == fst y) . sortOn fst
        isMonotonicStartingAt _ [] = True
        isMonotonicStartingAt k (k':ks) = k == k' && isMonotonicStartingAt (k + 1) ks
        baseIds = [(wid, -1) | wid <- [0 .. WorkerId nWorkers - 1]]
    ids <- withSchedulerWS state scheduleJobs
    length ids `shouldBe` n
    let gathered = gather (ids ++ baseIds)
    map (map snd) gathered `shouldSatisfy` all (isMonotonicStartingAt (-1))
    ids' <- withSchedulerWSR state scheduleJobs
    length ids' `shouldBe` n
    let gathered' = gather (baseIds ++ ids ++ F.toList ids')
    map (map snd) gathered' `shouldSatisfy` all (isMonotonicStartingAt (-1))
    withSchedulerWS_ state $ \schedulerWS -> do
      numWorkers (unwrapSchedulerWS schedulerWS) `shouldBe` nWorkers
      replicateM (10 * n) $
        scheduleWorkState_ schedulerWS $ \(wid, ref) -> do
          counter <- readIORef ref
          when (counter > 0) $ snd (last (gathered' !! getWorkerId wid)) `shouldBe` pred counter

prop_MutexException :: Comp -> Property
prop_MutexException comp =
  concurrentProperty $
  assertExceptionIO (== MutexException) $ do
    state <- initWorkerStates comp (pure . getWorkerId)
    withSchedulerWS_ state $ \schedulerWS ->
      scheduleWorkState_ schedulerWS $ \_s -> withSchedulerWS_ state $ \_s' -> pure ()

-- prop_CancelBatchAndResume :: Comp -> Int -> ([Int], [Int]) -> [Int] -> [Int] -> Property
-- prop_CancelBatchAndResume comp x' (xs1, xs2) ys zs =
--   concurrentPropertyIO $ do
--     res <-
--       withScheduler comp $ \s -> do
--         batchId <- getCurrentBatchId s
--         forM_ (concat [xs1, [x'], xs2]) $ \x -> scheduleWork s $ do
--           hasFinished <- hasBatchFinished s batchId
--           if hasFinished then pure Nothing $ do
--             if x == x'
--               then x <$ cancelBatchWith s x
--               else pure x
--         waitForBatchR s `shouldReturn` x'
--     pure (res === zs)

prop_FindCancelResume :: Comp -> Int64 -> ([Int64], [Int64]) -> [Int64] -> Property
prop_FindCancelResume comp x' (xs1', xs2') ys =
  concurrentExpectation $ do
    let f = (10 *)
        g = (100 *)
        xs1 = filter (/= x') xs1'
        xs2 = filter (/= x') xs2'
        xs = concat [xs1, [x'], xs2]
    res <-
      withSchedulerR comp $ \s -> do
        forM_ xs $ \x ->
          scheduleWork s $ do
            if x == x'
              then Just x <$ cancelBatchWith s (Just (f x))
              else pure Nothing
        waitForBatchR s `shouldReturn` FinishedEarlyWith (Just (f x'))
        forM_ ys (scheduleWork s . pure . Just)
        waitForBatchR s `shouldReturn` Finished (map Just ys)
        forM_ xs $ \x ->
          scheduleWork s $ do
            if x == x'
              then Just x <$ cancelBatch s (Just (g x))
              else pure $ Just (f x)
    case res of
      FinishedEarly rs r -> do
        r `shouldBe` Just (g x')
        rs `satisfyOrderedPartialPrefix` concat [map (Just . f) xs1, [Just x'], map (Just . f) xs2]
      fr -> expectationFailure $ "Unexpected result: " ++ show fr
  where
    satisfyOrderedPartialPrefix as bs =
      unless (orderedPartialPrefixOf as bs) $
      expectationFailure $
      "Expected " ++
      show as ++ " to be prefix of " ++ show bs ++ " possibly with some elements skipped"
    -- Make sure the first list is the prefix of the second
    orderedPartialPrefixOf [] _ = True
    orderedPartialPrefixOf (_:_) [] = False
    orderedPartialPrefixOf (a:as) (b:bs)
      | a == b = orderedPartialPrefixOf as bs
      | otherwise = orderedPartialPrefixOf (a : as) bs

spec :: Spec
spec = do
  describe "Comp" $ do
    describe "Monoid" $ do
      it "x <> mempty = x" $ property $ \(x :: Comp) -> x <> mempty === x
      it "mempty <> x = x" $ property $ \(x :: Comp) -> mempty <> x === x
      it "x <> (y <> z) = (x <> y) <> z" $
        property $ \(x :: Comp) y z -> x <> (y <> z) === (x <> y) <> z
      it "mconcat = foldr '(<>)' mempty" $
        property $ \(xs :: [Comp]) -> mconcat xs === foldr (<>) mempty xs
      eqSpecOnArbitrary @Comp
      monoidSpecOnArbitrary @Comp
    describe "Show" $ do
      it "show == showsPrec 0" $ property $ \(x :: Comp) -> x `deepseq` show x === showsPrec 0 x ""
      it "(show) == showsPrec 1" $
        property $ \(x :: Comp) (Positive n) ->
          x /= Seq && x /= Par ==> ("(" <> show x <> ")" === showsPrec n x "")
  describe "Results" $ do
    eqSpecOnArbitrary @(Results Int)
    functorSpecOnArbitrary @Results
    showReadSpecOnArbitrary @(Results Int)
    it "Traversable" $ property $ \(rs :: Results Int) (f :: Fun Int (Maybe Int)) ->
      traverse (apply f) (F.toList rs) === fmap F.toList (traverse (apply f) rs)
  describe "WorkerId" $ do
    eqSpecOnArbitrary @WorkerId
    ordSpecOnArbitrary @WorkerId
    it "MaxMin" $ property $ \x y ->
      conjoin [ max (WorkerId x) (WorkerId y) === WorkerId (max x y)
              , min (WorkerId x) (WorkerId y) === WorkerId (min x y)
              ]
    showReadSpecOnArbitrary @WorkerId
    describe "Enum" $ do
      it "toEnumFromEnum" $ property $ \ wid@(WorkerId i) ->
        toEnum (getWorkerId wid) === wid .&&. fromEnum wid === i
      it "succ . pred" $ property $ \ wid@(WorkerId i) ->
        i /= minBound && i /= maxBound ==>
        succ (pred wid) === wid .&&. pred (succ wid) === wid
  describe "Trivial" $ do
    it "WorkerIdIsZero" $ do
      scheduleWorkId trivialScheduler_ (`shouldBe` 0)
      withTrivialScheduler (`scheduleWorkId` pure) `shouldReturn` [0]
    it "TerminateDoesNothing" $ do
      terminate_ trivialScheduler_ `shouldReturn` ()
      terminate trivialScheduler_ () `shouldReturn` ()
      terminateWith trivialScheduler_ () `shouldReturn` ()
    prop "TerminateSeq" $ prop_Terminate withTrivialScheduler terminate (\xs x -> xs ++ [x])
    prop "TerminateWithSeq" $ prop_Terminate withTrivialScheduler terminateWith (\_ x -> [x])
    prop "TerminateSeqR" $ prop_Terminate withTrivialSchedulerR terminate FinishedEarly
    prop "TerminateWithSeqR" $
      prop_Terminate withTrivialSchedulerR terminateWith (const FinishedEarlyWith)
  describe "Seq" $ do
    prop "SameList" $ prop_SameList Seq
    prop "Recursive" $ prop_Recursive Seq
    prop "Nested" $ prop_Nested Seq
    prop "Serially" $ prop_Serially Seq
    prop "TrivialAsSeq_" prop_TrivialSchedulerSameAsSeq_
    prop "replicateConcurrently == replicateM" prop_ReplicateM
    prop "replicateConcurrently == replicateWork" prop_ReplicateWorkSeq
    it "WorkerIdIsZero" $
      withScheduler Seq (`scheduleWorkId` pure) `shouldReturn` [0]
    prop "TerminateSeq" $ prop_Terminate (withScheduler Seq) terminate (\xs x -> xs ++ [x])
    prop "TerminateWithSeq" $ prop_Terminate (withScheduler Seq) terminateWith (\_ x -> [x])
    prop "TerminateSeqR" $ prop_Terminate (withSchedulerR Seq) terminate FinishedEarly
    prop "TerminateWithSeqR" $
      prop_Terminate (withSchedulerR Seq) terminateWith (const FinishedEarlyWith)
  describe "ParOn" $ do
    prop "SameList" $ \cs -> prop_SameList (ParOn cs)
    prop "Recursive" $ \cs -> prop_Recursive (ParOn cs)
    prop "Nested" $ \cs -> prop_Nested (ParOn cs)
    prop "Serially" $ \cs -> prop_Serially (ParOn cs)
  describe "Arbitrary Comp" $ do
    prop "Trivial" prop_SameAsTrivialScheduler
    prop "ArbitraryCompNested" prop_ArbitraryCompNested
    prop "AllJobsProcessed" prop_AllJobsProcessed
    prop "traverseConcurrently == traverse" prop_Traverse
  describe "Exceptions" $ do
    prop "CatchDivideByZero" prop_CatchDivideByZero
    prop "CatchDivideByZeroNested" prop_CatchDivideByZeroNested
    prop "KillBlockedCoworker" prop_KillBlockedCoworker
    prop "KillSleepingCoworker" prop_KillSleepingCoworker
    prop "ExpectAsyncException" prop_ExpectAsyncException
    prop "WorkerCaughtAsyncException" prop_WorkerCaughtAsyncException
    prop "AllWorkersDied" prop_AllWorkersDied
    prop "traverseConcurrently_" prop_TraverseConcurrently_
    prop "traverseConcurrentlyInfinite_" prop_TraverseConcurrentlyInfinite_
  describe "Premature" $ do
    prop "FinishEarly" prop_FinishEarly
    prop "FinishEarly_" prop_FinishEarly_
    prop "FinishEarlyWith" prop_FinishEarlyWith
    prop "FinishBeforeStarting" prop_FinishBeforeStarting
    prop "FinishWithBeforeStarting" prop_FinishWithBeforeStarting
  describe "WorkerState" $ do
    prop "MutexException" prop_MutexException
    prop "WorkerStateExclusive" prop_WorkerStateExclusive
  describe "Restartable" $ do
    prop "ManyJobsInChunks" prop_ManyJobsInChunks
    prop "FindCancelResume" prop_FindCancelResume
    -- prop "CancelBatchAndResume" $ prop_CancelBatchAndResume
    -- prop "ManyJobsStoppedEarly" prop_ManyJobsStoppedEarly

instance Arbitrary WorkerId where
  arbitrary = WorkerId <$> arbitrary

instance Arbitrary a => Arbitrary (Results a) where
  arbitrary =
    oneof
      [ Finished <$> arbitrary
      , FinishedEarly <$> arbitrary <*> arbitrary
      , FinishedEarlyWith <$> arbitrary
      ]

-- | Assert a synchronous exception
assertExceptionIO :: (NFData a, Exception exc) =>
                     (exc -> Bool) -- ^ Return True if that is the exception that was expected
                  -> IO a -- ^ IO Action that should throw an exception
                  -> Property
assertExceptionIO isExc action =
  monadicIO $ do
    hasFailed <-
      run $
      catch
        (do res <- action
            res `deepseq` return False) $ \exc -> displayException exc `deepseq` return (isExc exc)
    assert hasFailed

assertAsyncExceptionIO :: (Exception e, NFData a) => (e -> Bool) -> IO a -> Property
assertAsyncExceptionIO isAsyncExc action =
  monadicIO $ do
    hasFailed <-
      run $
      EUnsafe.catch
        (do res <- action
            res `deepseq` return False)
        (\exc ->
           case EUnsafe.asyncExceptionFromException exc of
             Just asyncExc
               | isAsyncExc asyncExc -> displayException asyncExc `deepseq` pure True
             _ -> EUnsafe.throwIO exc)
    assert hasFailed
