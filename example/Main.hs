{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}

module Main where

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
  let esc = E.PG.EventStoreConfig
        { E.PG._escConnection = conn
        , E.PG._escTable      = PG.Types.QualifiedIdentifier Nothing "event"
        }
      env = Env
        { _eEventStoreConfig = esc
        }
  uuid <- U.V4.nextRandom
  runApp env $ do
    E.writeEvents @"store"
      [ E.WriteRecord "Account" uuid $ Ae.object
          [ "type" Ae..= ("AccountCreated" :: Tx.Text)
          , "value" Ae..= Ae.object
              [ "accountId" Ae..= uuid
              , "email" Ae..= ("user@example.com" :: Tx.Text)
              ]
          ]
      ]

newtype App a
  = App { _runApp :: ReaderT Env IO a }
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader Env)
  deriving (E.MonadEventStore "store")
    via (E.PG.EventStoreT "store" App)

data Env
  = Env
      { _eEventStoreConfig :: !(E.PG.EventStoreConfig "store")
      }

  deriving (Generic)

runApp :: Env -> App a -> IO a
runApp env (App m)
  = runReaderT m env
