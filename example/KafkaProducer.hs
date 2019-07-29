{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module KafkaProducer where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader, ReaderT (..))
import qualified Data.Aeson as Ae
import qualified Data.Text as Tx
import qualified Data.UUID as U
import qualified Data.UUID.V4 as U.V4
import qualified Everest as E
import qualified Everest.JSON as E.JSON
import qualified Everest.Kafka as E.K
import GHC.Generics (Generic)
import qualified Kafka.Producer as K.P

--------------------------------------------------------------------------------
--  Example producer
--------------------------------------------------------------------------------

main :: IO ()
main = do
  let prodProps = K.P.brokersList [K.P.BrokerAddress "localhost:9092"]
  eitherErrOrProd <- K.P.newProducer prodProps
  case eitherErrOrProd of
    Left err ->
      print err
    Right prod -> do
      let pc = E.K.ProducerConfig
            { E.K._pcProducer = prod
            }
          env = Env
            { _eProducerConfig = pc
            }
      uuid <- U.V4.nextRandom
      runApp env $ do
        E.writeEvents @"store"
          [ E.JSON.writeRecord (E.Topic "Account") "test-key" $
              AccountCreated AccountCreatedEvent
                { _aceAccountId = uuid
                , _aceEmail     = "user@example.com"
                }
          ]

data AccountEvent
  = AccountCreated AccountCreatedEvent
  | AccountDeleted AccountDeletedEvent
  deriving (Generic, Ae.ToJSON)

data AccountCreatedEvent
  = AccountCreatedEvent
      { _aceAccountId :: U.UUID
      , _aceEmail     :: Tx.Text
      }
  deriving (Generic, Ae.ToJSON)

data AccountDeletedEvent
  = AccountDeletedEvent
      { _adeAccountId :: U.UUID
      }
  deriving (Generic, Ae.ToJSON)

newtype App a
  = App { _runApp :: ReaderT Env IO a }
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader Env)
  deriving (E.MonadProducibleEventStore "store")
    via (E.K.ProducerT "store" App)

data Env
  = Env
      { _eProducerConfig :: E.K.ProducerConfig "store"
      }

  deriving stock Generic

runApp :: Env -> App a -> IO a
runApp env (App m)
  = runReaderT m env
