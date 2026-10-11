module Scheduler.Bell where

import Control.Concurrent.MVar (MVar, tryPutMVar, newMVar, takeMVar)
import Control.Monad (void)

newtype Bell = Bell (MVar ())

await :: Bell -> IO ()
await (Bell m) = takeMVar m

ring :: Bell -> IO ()
ring (Bell m) = void . tryPutMVar m $ ()

newBell :: IO Bell
newBell = Bell <$> newMVar ()
