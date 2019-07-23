{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Everest.PostgreSQL where

import qualified Everest as E

import qualified Conduit as Cdt
import qualified Control.Lens as Lens
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader)
import Control.Monad.Trans.Class (lift)
import qualified Data.Aeson as Ae
import qualified Data.ByteString.Char8 as BS.C8
import Data.Coerce (coerce)
import Data.Functor (void)
import qualified Data.Generics.Product.Typed as G.P
import Data.Int (Int64)
import Data.Proxy (Proxy)
import qualified Data.Text as Tx
import qualified Data.UUID as U
import qualified Database.PostgreSQL.Simple as PG
import qualified Database.PostgreSQL.Simple.Notification as PG.N
import qualified Database.PostgreSQL.Simple.Types as PG.Types
import GHC.Generics (Generic)
import Text.Read (readMaybe)

newtype ProducerT tag m a
  = ProducerT (m a)
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader r)

data ProducerConfig tag
  = ProducerConfig
      { _pcConnection :: !PG.Connection
      , _pcTable      :: !PG.Types.QualifiedIdentifier
      }
  deriving (Generic)

instance ( MonadIO m
         , MonadReader r m
         , G.P.HasType (ProducerConfig tag) r
         )
      => E.MonadProducibleEventStore tag (ProducerT tag m) where
  type Key tag (ProducerT tag m)
    = U.UUID
  type Value tag (ProducerT tag m)
    = Ae.Value
  writeEvents' =
    pgWriteEvents'
  {-# INLINE writeEvents' #-}

pgWriteEvents'
  :: forall tag r m
   . ( MonadIO m
     , MonadReader r m
     , G.P.HasType (ProducerConfig tag) r
     )
  => Proxy tag
  -> [E.WriteRecord U.UUID Ae.Value]
  -> ProducerT tag m ()
pgWriteEvents' _prx wrs = do
  cfg <- Lens.view (G.P.typed @(ProducerConfig tag))
  let conn = _pcConnection cfg
      tbl  = _pcTable cfg
  void $ liftIO $ PG.execute conn
    "insert into ? (topic, aggregate_id, data) ? on conflict do nothing"
    ( tbl
    , PG.Types.Values ["text", "uuid", "jsonb"]
        [(topic, k, v) | E.WriteRecord (E.Topic topic) k v <- wrs]
    )
  pure ()

newtype ConsumerT tag m a
  = ConsumerT (m a)
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader r)

data ConsumerConfig tag
  = ConsumerConfig
      { _ccConnection :: !PG.Connection
      , _ccTable      :: !PG.Types.QualifiedIdentifier
      , _ccChannel    :: !Tx.Text
      , _ccOffset     :: !E.Offset
      }
  deriving (Generic)

instance ( MonadIO m
         , MonadReader r m
         , G.P.HasType (ConsumerConfig tag) r
         )
      => E.MonadConsumableEventStore tag (ConsumerT tag m) where
  allEvents' =
    pgAllEvents'
  {-# INLINE allEvents' #-}

pgAllEvents'
  :: forall tag r m i
   . ( MonadIO m
     , MonadReader r m
     , G.P.HasType (ConsumerConfig tag) r
     )
  => Proxy tag
  -> [E.Topic]
  -> Cdt.ConduitT
      i
      (E.ReadRecord (E.Key tag (ConsumerT tag m)) (E.Value tag (ConsumerT tag m)))
      (ConsumerT tag m)
      ()
pgAllEvents' _prx topics = do
  (conn, tbl, chan, offset, maxOffset) <- lift $ do
    cfg <- Lens.view (G.P.typed @(ConsumerConfig tag))
    let conn   = _ccConnection cfg
        tbl    = _ccTable cfg
        chan   = _ccChannel cfg
        offset = _ccOffset cfg
    maxOffset <- liftIO $ do
      pgListen conn chan
      pgGetMaxOffset conn tbl
    pure (conn, tbl, chan, offset, maxOffset)
  goCatchUp conn tbl offset maxOffset
  goListen conn tbl chan
  where
    goCatchUp conn tbl offset maxOffset = do
      let query =
             "select (offset, type, aggregate_id, data)\
            \ from ?\
            \ where offset >= ?\
            \ and offset <= ?\
            \ and type in ?\
            \ order by offset asc"
          params =
            ( tbl
            , E.getOffset offset
            , E.getOffset maxOffset
            , PG.In (coerce @[E.Topic] @[Tx.Text] topics)
            )
      pgStreamQuery conn query params
    goListen conn tbl chan = forever $ do
      notifiedOffsets <- liftIO $ pgDrainNotifications conn chan
      undefined

pgStreamQuery
  :: ( PG.ToRow q
     )
  => PG.Connection
  -> PG.Query
  -> q
  -> Cdt.ConduitT
      i
      o
      (ConsumerT tag m)
      ()
pgStreamQuery conn query params = do
  undefined

pgListen
  :: PG.Connection
  -> Tx.Text
  -> IO ()
pgListen conn chan =
  void $ PG.execute conn "listen ?" (PG.Only chan)

pgGetMaxOffset
  :: PG.Connection
  -> PG.Types.QualifiedIdentifier
  -> IO E.Offset
pgGetMaxOffset conn tbl =
  PG.query conn "select coalesce(max(offset), 0) from ?" (PG.Only tbl) >>= \case
    [PG.Only offset] ->
      pure (E.Offset offset)
    _ ->
      error "Failable pattern in block with no MonadFail instance"

pgDrainNotifications conn chan = do
  n <- PG.N.getNotification conn
  case readOffset n of
    Just offset ->
      drain conn [offset]
    Nothing -> do
      -- TODO: ERRORS
      pure []
  where
    readOffset :: PG.N.Notification -> Maybe E.Offset
    readOffset (PG.N.Notification _ _ offsetBS) =
      coerce @(_ Int64) (readMaybe (BS.C8.unpack offsetBS))
    drain conn offsets =
      PG.N.getNotificationNonBlocking conn >>= \case
        Just n
          | Just offset <- readOffset n ->
              drain conn (offset : offsets)
          | otherwise -> do
              -- TODO: ERRORS
              pure offsets
        Nothing ->
          pure offsets

