"""french-factors: the parser on the head and tail of the three files as the
library served them on 11 October 2026, and the source's contract."""

from datetime import date

import pandas as pd
import pytest

from data_ingest.core.source import Window
from data_ingest.core.spec import WriteMode
from data_ingest.sources import french_factors as ff
from data_ingest.sources.french_factors import FrenchFactors, parse_factors

FF3 = """This file was created by using the 202608 CRSP database.
The Tbill return is the simple daily rate that, over the number of trading days
compounds to 1-month TBill rate. The 1-month TBill rate data until 202405 are from Ibbotson Associates. Starting from 202406, the 1-month TBill rate is from ICE BofA US 1-Month Treasury Bill Index.

,Mkt-RF,SMB,HML,RF
19260701,    0.09,   -0.23,   -0.28,    0.01
19260702,    0.45,   -0.34,   -0.03,    0.01
20260831,   -0.33,   -0.02,   -0.39,    0.01

Copyright 2026 Eugene F. Fama and Kenneth R. French
"""

FF5 = """This file was created by using the 202608 CRSP database.

,Mkt-RF,SMB,HML,RMW,CMA,RF
19630701,   -0.67,    0.00,   -0.33,   -0.01,    0.16,    0.01
20260831,   -0.33,   -0.29,   -0.39,   -1.09,    0.03,    0.01

Copyright 2026 Eugene F. Fama and Kenneth R. French
"""

MOMENTUM = """This file was created by using the 202608 CRSP database.  It
contains a momentum factor, constructed from six value-weight portfolios formed
the 70th NYSE percentile.

Missing data are indicated by -99.99 or -999.

,Mom
19261103,   0.55
19261104,  -99.99
19261105,  -999
20260831,  -0.14

Copyright 2026 Eugene F. Fama and Kenneth R. French
"""


def test_three_factor_file():
    frame = parse_factors(FF3, "ff3")
    assert list(frame.columns) == ["dataset", "factor", "date", "value"]
    assert len(frame) == 12 and set(frame["dataset"]) == {"ff3"}
    first = frame[frame["date"] == date(1926, 7, 1)].set_index("factor")["value"]
    assert first.to_dict() == {"Mkt-RF": 0.09, "SMB": -0.23, "HML": -0.28, "RF": 0.01}
    assert frame["date"].max() == date(2026, 8, 31)


def test_five_factor_file_has_its_own_smb():
    five = parse_factors(FF5, "ff5").set_index(["date", "factor"])["value"]
    three = parse_factors(FF3, "ff3").set_index(["date", "factor"])["value"]
    assert sorted(five.loc[date(1963, 7, 1)].index) == ["CMA", "HML", "Mkt-RF", "RF", "RMW", "SMB"]
    day = date(2026, 8, 31)
    assert five.loc[(day, "SMB")] == -0.29 and three.loc[(day, "SMB")] == -0.02
    assert five.loc[(day, "Mkt-RF")] == three.loc[(day, "Mkt-RF")]


def test_momentum_is_stored_as_umd_and_a_missing_value_is_a_missing_row():
    frame = parse_factors(MOMENTUM, "momentum")
    assert set(frame["factor"]) == {"UMD"}
    assert frame["date"].tolist() == [date(1926, 11, 3), date(2026, 8, 31)]
    assert frame["value"].tolist() == [0.55, -0.14]


def test_a_block_after_the_daily_one_is_not_read():
    # The monthly files of the library carry an annual block after a blank
    # line; a daily file that grew one must not have it read as days.
    text = FF3.replace("\nCopyright", "\n Annual Factors: January-December\n,Mkt-RF,SMB,HML,RF\n  1927,   29.47,   -2.04,   -4.54,    3.12\n\nCopyright")
    assert len(parse_factors(text, "ff3")) == 12


def test_a_file_without_a_header_is_an_error():
    with pytest.raises(ValueError, match="no header"):
        parse_factors("<html>moved</html>", "ff3")


def test_source_contract(mocker):
    texts = {"ff3": FF3, "ff5": FF5, "momentum": MOMENTUM}
    mocker.patch.object(ff, "download", side_effect=lambda dataset: texts[dataset])
    source = FrenchFactors()
    frame = source.fetch(Window.recent(0))
    assert source.write_mode is WriteMode.VERSIONED
    assert list(frame.columns) == source.table.column_names
    assert len(frame) == 12 + 12 + 2
    assert not frame.duplicated(list(source.table.primary_key)).any()
    assert set(frame["factor"]) == {"Mkt-RF", "SMB", "HML", "RMW", "CMA", "UMD", "RF"}


@pytest.mark.network
def test_the_library_still_serves_the_three_files():
    for dataset in ff.DATASETS:
        frame = parse_factors(ff.download(dataset), dataset)
        assert len(frame) > 15000
        assert frame["date"].max() > date(2026, 1, 1)
        assert pd.Series(frame["value"]).abs().max() < 40  # percent, not decimals nor a missing code
