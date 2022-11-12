{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass #-}

module Main where

import Control.Applicative
import Data.Aeson
import Data.Int
import Data.Text (Text)
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow
import GHC.Generics
import System.Posix.Env.ByteString (getArgs)
import Text.Read (readMaybe)

-- |Uses read instance of a type to parse the value. Sometimes useful
-- to dig numbers from a string type.
mRead :: (Alternative m, Read a) => String -> m a 
mRead = maybe empty pure . readMaybe

data Channel = Channel { chanId   :: Text
                       , remote   :: Text
                       , private  :: Bool
                       , active   :: Bool
                       , capacity :: Integer
                       , local    :: Integer
                       , uptime   :: Text
                       , lifetime :: Text
                       } deriving (Show, Generic, ToRow)

instance FromField Channel where
  fromField = fromJSONField

instance FromJSON Channel where
  parseJSON = withObject "Channel" $ \o -> Channel
    <$> o .: "chan_id"
    <*> o .: "remote_pubkey"
    <*> o .: "private"
    <*> o .: "active"
    <*> (o .: "capacity" >>= mRead)
    <*> (o .: "local_balance" >>= mRead)
    <*> o .: "uptime"
    <*> o .: "lifetime"

main = do
  [dbConnStr] <- getArgs
  conn <- connectPostgreSQL dbConnStr
  materialize conn >>= print

materialize :: Connection -> IO Int64
materialize conn = do
  xs <- query_ conn "select run_id,o from cmd_json where run_id between 323 and 325 and o->>'remote_pubkey' = '03808b1a698906bbf0457922aec66168a8edbfe98b1379249ace270b41ae0dde48'"
  executeMany conn "INSERT INTO chan_harjoitus VALUES (?,?,?,?,?,?,?,?,?)" (map mankeloi xs)

mankeloi :: (Int, Channel) -> [Action]
mankeloi (rowId, chan) = toField rowId : toRow chan
