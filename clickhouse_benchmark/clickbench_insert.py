from pathlib import Path

from clickhouse_benchmark.clickbench import download_file
from clickhouse_benchmark.client import ClickHouseClient
from clickhouse_benchmark.config import Config
from clickhouse_benchmark.results import InsertBenchmarkResult, get_query_statistics
from clickhouse_benchmark.service_matrix import Service


def run_insert(service: Service, config: Config) -> InsertBenchmarkResult:
    download_file()

    client = service.client
    client.execute_no_result(
        """
        CREATE TABLE IF NOT EXISTS hits
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
    query_ids = []
    threads = get_cpu_count(client)
    for _ in range(config.query_run_count):
        query_ids.append(
            client.execute_no_result(
                """INSERT INTO hits FORMAT TSV""",
                input=Path("hits.tsv"),
                settings={"max_insert_threads": threads},
            )
        )
        client.execute_no_result("TRUNCATE TABLE hits")
    client.execute_all_nodes("SYSTEM FLUSH LOGS")
    return InsertBenchmarkResult(
        plan=service.plan,
        results=get_query_statistics(client, query_ids),
    )


def get_cpu_count(client: ClickHouseClient) -> int:
    return int(
        client.execute(
            "SELECT value FROM system.settings WHERE name = 'max_threads'"
        ).results[0]["value"]
    )
