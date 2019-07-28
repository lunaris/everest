{-# LANGUAGE AllowAmbiguousTypes #-}
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
import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.STM as STM
import qualified Control.Concurrent.STM.TBMQueue as STM.TBMQ
import Control.Monad (forever)
import Control.Monad.Catch (finally)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader)
import Control.Monad.Trans.Class (lift)
import qualified Control.Monad.Trans.Resource as Res
import qualified Data.Aeson as Ae
import qualified Data.ByteString.Char8 as BS.C8
import Data.Coerce (Coercible, coerce)
import Data.Functor (void)
import qualified Data.Generics.Product.Typed as G.P
import Data.Int (Int64)
import Data.Proxy (Proxy)
import Data.String (fromString)
import qualified Data.Text as Tx
import qualified Data.UUID as U
import qualified Database.PostgreSQL.Simple as PG
import qualified Database.PostgreSQL.Simple.FromRow as PG.From
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
  type ProducerKey tag (ProducerT tag m)
    = U.UUID
  type ProducerValue tag (ProducerT tag m)
    = Ae.Value
  writeEventsP =
    pgWriteEventsP
  {-# INLINE writeEventsP #-}

pgWriteEventsP
  :: forall tag r m
   . ( MonadIO m
     , MonadReader r m
     , G.P.HasType (ProducerConfig tag) r
     )
  => Proxy tag
  -> [E.WriteRecord U.UUID Ae.Value]
  -> ProducerT tag m ()
pgWriteEventsP _ptag wrs = do
  cfg <- Lens.view (G.P.typed @(ProducerConfig tag))
  let conn = _pcConnection cfg
      tbl  = _pcTable cfg
  void $ liftIO $ PG.execute conn
    "insert into ? (topic, key, value) ? on conflict do nothing"
    ( tbl
    , PG.Types.Values ["text", "uuid", "jsonb"]
        [(topic, k, v) | E.WriteRecord (E.Topic topic) k v <- wrs]
    )

newtype ConsumerT tag m a
  = ConsumerT (m a)
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, Res.MonadResource, MonadReader r)

data ConsumerConfig tag
  = ConsumerConfig
      { _ccConnection :: !PG.Connection
      , _ccTable      :: !PG.Types.QualifiedIdentifier
      , _ccChannel    :: !Tx.Text
      , _ccOffset     :: !E.Offset
      }
  deriving (Generic)

instance ( MonadIO m
         , Res.MonadResource m
         , MonadReader r m
         , G.P.HasType (ConsumerConfig tag) r
         )
      => E.MonadConsumableEventStore tag (ConsumerT tag m) where
  type ConsumerMonad tag (ConsumerT tag m)
    = m
  type ConsumerKey tag (ConsumerT tag m)
    = U.UUID
  type ConsumerValue tag (ConsumerT tag m)
    = Ae.Value
  allEventsP =
    pgAllEventsP
  {-# INLINE allEventsP #-}

pgAllEventsP
  :: forall tag r m i
   . ( MonadIO m
     , Res.MonadResource m
     , MonadReader r m
     , G.P.HasType (ConsumerConfig tag) r
     )
  => Proxy tag
  -> Proxy (ConsumerT tag m)
  -> [E.Topic]
  -> Cdt.ConduitT i (E.ReadRecord U.UUID Ae.Value) m ()
pgAllEventsP _ptag _pm topics = do
  (conn, tbl, offset, maxOffset) <- lift $ do
    cfg <- Lens.view (G.P.typed @(ConsumerConfig tag))
    let conn   = _ccConnection cfg
        tbl    = _ccTable cfg
        chan   = _ccChannel cfg
        offset = _ccOffset cfg
    maxOffset <- liftIO $ do
      pgListen conn chan
      pgGetMaxOffset conn tbl
    pure (conn, tbl, offset, maxOffset)
  goCatchUp conn tbl offset maxOffset
  goListen conn tbl
  where
    goCatchUp conn tbl offset maxOffset = do
      let query =
             "select _offset, _timestamp, topic, key, value\
            \ from ?\
            \ where _offset >= ?\
            \ and _offset <= ?\
            \ and topic in ?\
            \ order by _offset asc"
          params =
            ( tbl
            , E.getOffset offset
            , E.getOffset maxOffset
            , PG.In (coerce @_ @[Tx.Text] topics)
            )
      pgStreamQuery @PGReadRecord conn query params
    goListen conn tbl = forever $ do
      notifiedOffsets <- liftIO $ pgDrainNotifications conn
      let query =
             "with es as (\
            \   select *\
            \   from ?\
            \   where _offset in ?\
            \   order by _offset asc\
            \ )\
            \ select _offset, _timestamp, topic, key, value\
            \ from es\
            \ where topic in ?\
            \ order by _offset asc"
          params =
            ( tbl
            , PG.In (coerce @_ @[Int64] notifiedOffsets)
            , PG.In (coerce @_ @[Tx.Text] topics)
            )
      evs <- liftIO $ PG.query conn query params
      mapM_ Cdt.yield (coerce @[PGReadRecord] @[_] evs)

pgStreamQuery
  :: forall r i o q m
   . ( MonadIO m
     , Res.MonadResource m
     , PG.ToRow q
     , PG.FromRow r
     , Coercible r o
     )
  => PG.Connection
  -> PG.Query
  -> q
  -> Cdt.ConduitT i o m ()
pgStreamQuery conn query params = do
  queue <- liftIO $ STM.TBMQ.newTBMQueueIO capacity
  asyncFetcher <- launchAsync $
    fetchAllAndSendTo queue `finally` close queue
  yieldAllFrom queue
  liftIO $ Async.wait asyncFetcher
  where
    capacity =
      32 * 1024
    launchAsync m =
      fmap snd $ Res.liftResourceT $
        Res.allocate (Async.async m) Async.cancel
    fetchAllAndSendTo queue =
      PG.forEach conn query params
        (STM.atomically . STM.TBMQ.writeTBMQueue queue)
    close =
      STM.atomically . STM.TBMQ.closeTBMQueue
    yieldAllFrom queue = go
      where
        go =
          liftIO (STM.atomically (STM.TBMQ.readTBMQueue queue)) >>= \case
            Nothing ->
              pure ()
            Just r -> do
              Cdt.yield (coerce @r @o r)
              go

pgListen
  :: PG.Connection
  -> Tx.Text
  -> IO ()
pgListen conn chan =
  void $ PG.execute_ conn $ "listen " <> fromString (Tx.unpack chan)

pgGetMaxOffset
  :: PG.Connection
  -> PG.Types.QualifiedIdentifier
  -> IO E.Offset
pgGetMaxOffset conn tbl =
  PG.query conn "select coalesce(max(_offset), 0) from ?" (PG.Only tbl) >>= \case
    [PG.Only offset] ->
      pure (E.Offset offset)
    _ ->
      error "Failable pattern in block with no MonadFail instance"

pgDrainNotifications
  :: PG.Connection
  -> IO [E.Offset]
pgDrainNotifications conn = do
  n <- PG.N.getNotification conn
  case readOffset n of
    Just offset ->
      drain [offset]
    Nothing -> do
      -- TODO: ERRORS
      pure []
  where
    readOffset :: PG.N.Notification -> Maybe E.Offset
    readOffset (PG.N.Notification _ _ offsetBS) =
      coerce @(_ Int64) (readMaybe (BS.C8.unpack offsetBS))
    drain offsets =
      PG.N.getNotificationNonBlocking conn >>= \case
        Just n
          | Just offset <- readOffset n ->
              drain (offset : offsets)
          | otherwise -> do
              -- TODO: ERRORS
              pure offsets
        Nothing ->
          pure offsets

newtype PGReadRecord
  = PGReadRecord (E.ReadRecord U.UUID Ae.Value)

instance PG.From.FromRow PGReadRecord where
  fromRow = do
    offset <- PG.From.field
    timestamp <- PG.From.field
    topic <- PG.From.field
    key <- PG.From.field
    value <- PG.From.field
    pure $ PGReadRecord E.ReadRecord
      { E._rrTopic     = E.Topic topic
      , E._rrPartition = E.Partition 0
      , E._rrOffset    = E.Offset offset
      , E._rrTimestamp = timestamp
      , E._rrKey       = key
      , E._rrValue     = value
      }
