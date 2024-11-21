import gzip
import logging
import threading
from pathlib import Path

import requests

from clickhouse_benchmark.config import Config
from clickhouse_benchmark.service_matrix import Service

LOG = logging.getLogger(__name__)

URL = "https://clickhouse-public-datasets.s3.amazonaws.com/hits_compatible/hits.tsv.gz"
N_ROWS = 10_000_000

LOCK_DOWNLOAD_FILE = threading.Lock()

# We can saturate bandwidth more with multiple threads here
LOCK_UPLOAD_FILE = threading.Semaphore(3)


def setup(service: Service, config: Config) -> None:
    client = service.client
    client.execute_no_result("DROP TABLE IF EXISTS hits")
    client.execute_no_result(
        """
        CREATE TABLE hits
        (
            WatchID BIGINT NOT NULL,
            JavaEnable SMALLINT NOT NULL,
            Title TEXT NOT NULL,
            GoodEvent SMALLINT NOT NULL,
            EventTime TIMESTAMP NOT NULL,
            EventDate Date NOT NULL,
            CounterID INTEGER NOT NULL,
            ClientIP INTEGER NOT NULL,
            RegionID INTEGER NOT NULL,
            UserID BIGINT NOT NULL,
            CounterClass SMALLINT NOT NULL,
            OS SMALLINT NOT NULL,
            UserAgent SMALLINT NOT NULL,
            URL TEXT NOT NULL,
            Referer TEXT NOT NULL,
            IsRefresh SMALLINT NOT NULL,
            RefererCategoryID SMALLINT NOT NULL,
            RefererRegionID INTEGER NOT NULL,
            URLCategoryID SMALLINT NOT NULL,
            URLRegionID INTEGER NOT NULL,
            ResolutionWidth SMALLINT NOT NULL,
            ResolutionHeight SMALLINT NOT NULL,
            ResolutionDepth SMALLINT NOT NULL,
            FlashMajor SMALLINT NOT NULL,
            FlashMinor SMALLINT NOT NULL,
            FlashMinor2 TEXT NOT NULL,
            NetMajor SMALLINT NOT NULL,
            NetMinor SMALLINT NOT NULL,
            UserAgentMajor SMALLINT NOT NULL,
            UserAgentMinor VARCHAR(255) NOT NULL,
            CookieEnable SMALLINT NOT NULL,
            JavascriptEnable SMALLINT NOT NULL,
            IsMobile SMALLINT NOT NULL,
            MobilePhone SMALLINT NOT NULL,
            MobilePhoneModel TEXT NOT NULL,
            Params TEXT NOT NULL,
            IPNetworkID INTEGER NOT NULL,
            TraficSourceID SMALLINT NOT NULL,
            SearchEngineID SMALLINT NOT NULL,
            SearchPhrase TEXT NOT NULL,
            AdvEngineID SMALLINT NOT NULL,
            IsArtifical SMALLINT NOT NULL,
            WindowClientWidth SMALLINT NOT NULL,
            WindowClientHeight SMALLINT NOT NULL,
            ClientTimeZone SMALLINT NOT NULL,
            ClientEventTime TIMESTAMP NOT NULL,
            SilverlightVersion1 SMALLINT NOT NULL,
            SilverlightVersion2 SMALLINT NOT NULL,
            SilverlightVersion3 INTEGER NOT NULL,
            SilverlightVersion4 SMALLINT NOT NULL,
            PageCharset TEXT NOT NULL,
            CodeVersion INTEGER NOT NULL,
            IsLink SMALLINT NOT NULL,
            IsDownload SMALLINT NOT NULL,
            IsNotBounce SMALLINT NOT NULL,
            FUniqID BIGINT NOT NULL,
            OriginalURL TEXT NOT NULL,
            HID INTEGER NOT NULL,
            IsOldCounter SMALLINT NOT NULL,
            IsEvent SMALLINT NOT NULL,
            IsParameter SMALLINT NOT NULL,
            DontCountHits SMALLINT NOT NULL,
            WithHash SMALLINT NOT NULL,
            HitColor CHAR NOT NULL,
            LocalEventTime TIMESTAMP NOT NULL,
            Age SMALLINT NOT NULL,
            Sex SMALLINT NOT NULL,
            Income SMALLINT NOT NULL,
            Interests SMALLINT NOT NULL,
            Robotness SMALLINT NOT NULL,
            RemoteIP INTEGER NOT NULL,
            WindowName INTEGER NOT NULL,
            OpenerName INTEGER NOT NULL,
            HistoryLength SMALLINT NOT NULL,
            BrowserLanguage TEXT NOT NULL,
            BrowserCountry TEXT NOT NULL,
            SocialNetwork TEXT NOT NULL,
            SocialAction TEXT NOT NULL,
            HTTPError SMALLINT NOT NULL,
            SendTiming INTEGER NOT NULL,
            DNSTiming INTEGER NOT NULL,
            ConnectTiming INTEGER NOT NULL,
            ResponseStartTiming INTEGER NOT NULL,
            ResponseEndTiming INTEGER NOT NULL,
            FetchTiming INTEGER NOT NULL,
            SocialSourceNetworkID SMALLINT NOT NULL,
            SocialSourcePage TEXT NOT NULL,
            ParamPrice BIGINT NOT NULL,
            ParamOrderID TEXT NOT NULL,
            ParamCurrency TEXT NOT NULL,
            ParamCurrencyID SMALLINT NOT NULL,
            OpenstatServiceName TEXT NOT NULL,
            OpenstatCampaignID TEXT NOT NULL,
            OpenstatAdID TEXT NOT NULL,
            OpenstatSourceID TEXT NOT NULL,
            UTMSource TEXT NOT NULL,
            UTMMedium TEXT NOT NULL,
            UTMCampaign TEXT NOT NULL,
            UTMContent TEXT NOT NULL,
            UTMTerm TEXT NOT NULL,
            FromTag TEXT NOT NULL,
            HasGCLID SMALLINT NOT NULL,
            RefererHash BIGINT NOT NULL,
            URLHash BIGINT NOT NULL,
            CLID INTEGER NOT NULL,
            PRIMARY KEY (CounterID, EventDate, UserID, EventTime, WatchID)
        )
        ENGINE = MergeTree;
        """
    )

    with LOCK_DOWNLOAD_FILE:
        if not Path("hits.tsv").exists():
            LOG.info("Downloading hits data")
            response = requests.get(URL, stream=True)
            response.raise_for_status()

            try:
                with (
                    open("hits.tsv", "wb") as f_out,
                    gzip.GzipFile(fileobj=response.raw) as f_in,
                ):
                    for i, line in enumerate(f_in):
                        if i >= N_ROWS:
                            break
                        f_out.write(line)
            except RuntimeError:
                Path("hits.tsv").unlink()
                raise

    with LOCK_UPLOAD_FILE:
        LOG.info("Inserting hits data")
        client.execute_no_result(
            """INSERT INTO hits FORMAT TSV""",
            input=Path("hits.tsv"),
        )
