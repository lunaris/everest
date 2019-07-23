{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module Consumer where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader, ReaderT (..))
import qualified Data.Aeson as Ae
import qualified Data.Text as Tx
import qualified Data.UUID.V4 as U.V4
import qualified Database.PostgreSQL.Simple as PG
import qualified Database.PostgreSQL.Simple.Types as PG.Types
import qualified Everest as E
import qualified Everest.PostgreSQL as E.PG
import GHC.Generics (Generic)

--------------------------------------------------------------------------------
--  Example
--------------------------------------------------------------------------------

main :: IO ()
main = do
  conn <- PG.connect PG.ConnectInfo
    { PG.connectHost     = "localhost"
    , PG.connectPort     = 5432
    , PG.connectUser     = "everest"
    , PG.connectPassword = "everest"
    , PG.connectDatabase = "everest"
    }
  let cc = E.PG.ConsumerConfig
        { E.PG._ccConnection = conn
        , E.PG._ccTable      = PG.Types.QualifiedIdentifier Nothing "event"
        , E.PG._ccChannel    = "event"
        , E.PG._ccOffset     = E.Offset 0
        }
      env = Env
        { _eConsumerConfig = cc
        }
  uuid <- U.V4.nextRandom
  runApp env $ do
    undefined

newtype App a
  = App { _runApp :: ReaderT Env IO a }
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader Env)
  deriving (E.MonadConsumableEventStore "store")
    via (E.PG.ConsumerT "store" App)

data Env
  = Env
      { _eConsumerConfig :: !(E.PG.ConsumerConfig "store")
      }

  deriving (Generic)

runApp :: Env -> App a -> IO a
runApp env (App m)
  = runReaderT m env
