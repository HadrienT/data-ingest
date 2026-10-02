"""rating-default-rates: the parser on an answer recorded from ESMA's CEREP
(trimmed), the request it posts, and the source's contract. The `network`
test at the bottom asks CEREP for real."""

from datetime import date

import pandas as pd
import pytest

from data_ingest.core.source import Window
from data_ingest.sources import rating_default_rates as r
from data_ingest.sources.rating_default_rates import RatingDefaultRates


def _cell(defaults, rate):
    return {
        "nameOfRatingActivity": "",
        "numberOfRatings": defaults,
        "averageNumberOfRatings": 0.0,
        "percentageOfRatings": rate,
    }


# S&P, corporate, long-term, 2024 — as CEREP answered on 2 October 2026.
SP_2024 = {
    "ratingActivity": None,
    "defaultRates": {
        "defMap": {
            "AAA": _cell(0, 0.0),
            "BBB": _cell(0, 0.0),
            "BB": _cell(2, 0.21),
            "B": _cell(30, 1.88),
            "CCC": _cell(92, 28.52),
            "SD": _cell(1, 8.33),
        }
    },
    "lastPublicationDate": None,
    "emptyCatLabels": False,
}

FILTERS = {
    "filters": {
        "endOfPrd": {
            "filterList": [
                {"name": "31/12/2025", "code": "1767139200000", "selected": False},
                {"name": "30/06/2025", "code": "1751241600000", "selected": False},
                {"name": "31/12/2024", "code": "1735603200000", "selected": False},
                {"name": "31/12/2023", "code": "1703980800000", "selected": False},
                {"name": "30/06/1989", "code": "615168000000", "selected": False},
            ]
        }
    }
}


def test_a_year_of_default_rates_is_one_row_per_rating_category():
    rows = r.parse_default_rates(SP_2024, "S&P", 2024)
    assert rows[2] == ("S&P", date(2024, 1, 1), date(2024, 12, 31), "BB", 2, 0.21)
    assert [row[3] for row in rows] == ["AAA", "BBB", "BB", "B", "CCC", "SD"]
    # A category without a default is a zero, not a gap.
    assert rows[0][4:] == (0, 0.0)


def test_a_period_the_agency_did_not_report_gives_no_rows():
    assert r.parse_default_rates({"defaultRates": None}, "S&P", 1995) == []
    assert r.parse_default_rates({"defaultRates": {"defMap": {}}}, "S&P", 1995) == []
    assert r.parse_default_rates({}, "S&P", 1995) == []


def test_the_query_is_the_one_cereps_page_posts():
    q = r.query("STPGB", 2024)["filters"]
    # The codes CEREP lists for 01/01/2024 and 31/12/2024: midnight UTC in ms.
    assert q["begOfPrd"]["filterList"][0]["code"] == "1704067200000"
    assert q["endOfPrd"]["filterList"][0]["code"] == "1735603200000"
    assert q["cra"]["filterList"][0]["code"] == "STPGB"
    assert q["ratingType"]["filterList"][0]["code"] == "C"  # corporate
    assert q["timeHorizon"]["filterList"][0]["code"] == "L"  # long-term
    assert [f["code"] for f in q["categories"]["filterList"]] == ["C", "P"]


def test_only_whole_calendar_years_that_are_published_are_asked_for():
    assert r.published_years(FILTERS) == [2023, 2024, 2025]
    source = RatingDefaultRates()
    published = [1989, 2000, 2023, 2024, 2025]
    assert source.years(Window.everything(), published) == published
    window = Window(start=date(2024, 3, 1), end=date(2026, 10, 2))
    assert source.years(window, published) == [2024, 2025]


def test_fetch_returns_the_declared_columns(mocker):
    mocker.patch.object(r.time, "sleep")
    calls = []

    def post(path, body):
        calls.append(path)
        return FILTERS if path == "filters" else SP_2024

    mocker.patch.object(r, "_post", side_effect=post)
    frame = RatingDefaultRates().fetch(Window(start=date(2024, 1, 1), end=date(2024, 12, 31)))
    assert list(frame.columns) == RatingDefaultRates.table.column_names
    # One year, three agencies, six categories each.
    assert len(frame) == 18 and set(frame["agency"]) == {"S&P", "Moody's", "Fitch"}
    assert calls.count("searchStatistics/2") == 3
    assert not frame.duplicated(subset=list(RatingDefaultRates.table.primary_key)).any()
    assert pd.api.types.is_integer_dtype(frame["defaults"])


@pytest.mark.network
def test_cerep_answers_for_real():
    frame = RatingDefaultRates().fetch(Window(start=date(2024, 1, 1), end=date(2024, 12, 31)))
    sp = frame[frame["agency"] == "S&P"].set_index("rating")
    # Published and not expected to move: 30 single-B defaults in 2024.
    assert sp.loc["B", "defaults"] == 30 and sp.loc["B", "default_rate"] == pytest.approx(1.88)
    assert set(frame["agency"]) == {"S&P", "Moody's", "Fitch"}
