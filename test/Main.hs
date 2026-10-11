{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Scheduler
import Control.Concurrent (forkIO, threadDelay)

main :: IO ()
main = do
  scheduler <- initScheduler "data.db" :: IO (Scheduler String)
  _ <- forkIO $ runScheduler scheduler putStrLn

  scheduleJob scheduler (Just "test1") (Daily (TimeOfDay 9 0 0)) "t"

  scheduleJob scheduler (Just "test2") (Weekly Wednesday (TimeOfDay 9 0 0)) "h"

  scheduleJob scheduler (Just "test3") (NthWeekday 2 Friday 3 (TimeOfDay 9 0 0)) "hhh"

  r <- scheduleJob scheduler Nothing (Every 0.5) "this should print every 1/2 second!"
  print r

  scheduleJob scheduler (Just "5 seconds") (Every 5) "this should print every 5 seconds!"

  threadDelay (20 * 1000000)
  return ()
