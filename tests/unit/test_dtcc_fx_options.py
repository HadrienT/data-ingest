"""dtcc-fx-options: the parser on rows recorded from the real file of
2026-10-01 (columns trimmed to the ones read), and the source's contract. The
`network` test at the bottom downloads a real day."""

import csv
import io
from datetime import date, datetime, timedelta, timezone

import pytest

from data_ingest.core.source import Window
from data_ingest.sources import _dtcc_common as dtcc
from data_ingest.sources.dtcc_fx_options import DtccFxOptions, parse_fx_options

DAY = date(2026, 10, 1)

HEADER = (
    "Dissemination Identifier,Original Dissemination Identifier,Action type,Event type,Execution Timestamp,"
    "Effective Date,Expiration Date,Maturity date of the underlier,Notional amount-Leg 1,Notional amount-Leg 2,"
    "Notional currency-Leg 1,Notional currency-Leg 2,Call amount,Call currency,Put amount,Put currency,"
    "Option Premium Amount,Option Premium Currency,Strike Price,Strike price currency/currency pair,"
    "Strike price notation,Platform identifier,Package indicator,UPI FISN,UPI Underlier Name\n"
)

# A put on the euro traded on a platform and its amendment; the same kind of
# trade reported by a clearing entity, without its call amount; a capped one;
# a strike quoted the other way round; a call on the yen paid in yen; an
# exercise; a correction. Then what is not a vanilla option: a barrier, a
# non-deliverable option, a forward.
ROWS = """\
5574291761000000801,,NEWT,TRAD,2026-10-01T08:15:22Z,2026-10-01,2027-02-03,,"9,678,800","11,000,000",EUR,USD,10999956,USD,9678800,EUR,"170,588.85",USD,1.1365,EUR/USD,1,TSEF,TRUE,NA/O Van Put EUR USD,EUR USD
5574295990000000401,5574291761000000801,MODI,TRAD,2026-10-01T08:15:22Z,2026-10-01,2027-02-03,,"9,678,800","11,000,000",EUR,USD,10999956,USD,9678800,EUR,"170,588.85",USD,1.1365,EUR/USD,1,TSEF,TRUE,NA/O Van Put EUR USD,EUR USD
5579379812000000101,,NEWT,TRAD,2026-10-01T14:26:23Z,2026-10-01,2026-12-18,,"35,000,000","38,815,000",EUR,USD,,,35000000,EUR,"193,375",USD,1.109,EUR/USD,1,XLCH,TRUE,NA/O Van Put EUR USD,EUR USD
5586783298000000201,,NEWT,TRAD,2026-03-09T08:05:55Z,2026-09-30,2026-12-14,2026-12-16,"132,405,004+","132,405,004+",EUR,EUR,132405004+,EUR,132405004+,EUR,"1,088,796.31529",EUR,1.1,EUR/USD,1,BILT,TRUE,NA/O Van Put EUR USD,EUR USD
5579304505000000201,,NEWT,TRAD,2026-10-01T14:09:00Z,2026-10-01,2027-03-31,,"75,077","86,000",EUR,USD,75077,EUR,86000,USD,"1,284.8392",USD,1.1455,USD/EUR,1,BILT,FALSE,NA/O Van Call EUR USD,EUR USD
5575779451000000201,,NEWT,TRAD,2026-10-01T09:24:28Z,2026-10-01,2026-10-12,,"5,000,000","782,500,000",USD,JPY,,,5000000,USD,"2,234,485",JPY,156.5,USD/JPY,1,XXXX,FALSE,NA/O Van Call JPY USD,JPY USD
5579222555000000101,5077719980000001201,TERM,EXER,2026-09-02T04:05:50Z,2026-09-02,2026-10-01,,"3,400,000,000","29,824,500",JPY,AUD,3400000000,JPY,30000000,AUD,"251,271.4125",AUD,114,AUD/JPY,1,BILT,FALSE,NA/O Van Put AUD JPY,AUD JPY
5580520790000000101,5579994313000000401,CORR,,2026-10-01T15:56:58Z,2026-10-01,2026-12-18,2026-12-22,"75,000,000","51,000,000",AUD,USD,51000000,USD,75000000,AUD,"749,250",AUD,0.68,AUD/USD,1,BILT,TRUE,NA/O Van Put AUD USD,AUD USD
5572715687000000401,4599516666000000101,TERM,ETRM,2026-08-05T15:23:08Z,2026-08-05,2026-10-05,,"30,000,000","502,500,000",USD,MXN,30000000,USD,500000000,MXN,"602,400",USD,16.75,USD/MXN,1,BILT,FALSE,NA/O Bar Put MXN USD,MXN USD
5585343159000000301,5117861628000000501,TERM,EXER,2026-09-03T13:55:08Z,2026-10-01,2026-10-01,2026-10-05,"20,000,000","19,600,000,000",USD,CLP,20000000,USD,19600000000,CLP,"23,600",USD,980,USD/CLP,1,BTBS,TRUE,NA/O NDO Put CLP USD,CLP USD
5579233651000000401,,NEWT,TRAD,2026-10-01T13:52:33Z,2026-10-01,2027-01-27,,"29,960","34,000",EUR,USD,,,,,,,,,,360T,FALSE,NA/Fwd EUR USD,EUR USD
"""


def records(text=HEADER + ROWS):
    return list(csv.DictReader(io.StringIO(text)))


def parsed(text=HEADER + ROWS, day=DAY):
    columns = DtccFxOptions().table.column_names
    return [dict(zip(columns, row)) for row in parse_fx_options(records(text), day)]


def test_only_vanilla_options_are_kept_whatever_the_message():
    rows = parsed()
    # The barrier, the non-deliverable option and the forward are left out;
    # amendments, exercises and corrections are kept, as published.
    assert len(rows) == 8
    assert {r["product"].split(" ")[1] for r in rows} == {"Van"}
    assert [r["action_type"] for r in rows] == ["NEWT", "MODI", "NEWT", "NEWT", "NEWT", "NEWT", "TERM", "CORR"]
    assert rows[1]["original_dissemination_id"] == rows[0]["dissemination_id"]


def test_fields_are_stored_as_published():
    put = parsed()[0]
    assert put["report_date"] == DAY
    assert put["execution_timestamp"] == datetime(2026, 10, 1, 8, 15, 22, tzinfo=timezone.utc)
    assert (put["pair"], put["option_type"]) == ("EUR USD", "put")
    assert (put["effective_date"], put["expiry"], put["settlement_date"]) == (DAY, date(2027, 2, 3), None)
    assert (put["strike"], put["strike_pair"], put["strike_notation"]) == (1.1365, "EUR/USD", "1")
    assert (put["notional_1"], put["currency_1"]) == (9_678_800.0, "EUR")
    assert (put["notional_2"], put["currency_2"]) == (11_000_000.0, "USD")
    assert (put["premium"], put["premium_currency"]) == (170_588.85, "USD")
    assert (put["platform"], put["package"], put["notional_capped"]) == ("TSEF", True, False)


def test_the_option_type_is_about_the_first_currency_of_the_pair():
    rows = parsed()
    # A put on EUR USD delivers euros and receives dollars; the amounts'
    # ratio is the strike in dollars per euro.
    put = rows[0]
    assert (put["put_currency"], put["call_currency"]) == ("EUR", "USD")
    assert put["call_amount"] / put["put_amount"] == pytest.approx(put["strike"], rel=1e-5)
    # A call on JPY USD is a put on the dollar, struck in yen per dollar.
    yen = rows[5]
    assert (yen["pair"], yen["option_type"], yen["put_currency"]) == ("JPY USD", "call", "USD")
    assert (yen["strike"], yen["strike_pair"], yen["premium_currency"]) == (156.5, "USD/JPY", "JPY")
    # The strike's pair is as published, not normalised: this row says
    # USD/EUR for a strike that is dollars per euro, as its amounts show.
    odd = rows[4]
    assert (odd["strike"], odd["strike_pair"]) == (1.1455, "USD/EUR")
    assert odd["put_amount"] / odd["call_amount"] == pytest.approx(odd["strike"], rel=1e-4)


def test_capped_amounts_are_flagged_and_missing_ones_are_none():
    rows = parsed()
    capped = rows[3]
    assert capped["notional_capped"] and capped["notional_1"] == 132_405_004.0
    assert capped["premium"] == pytest.approx(1_088_796.31529)  # published in full
    # Reported by a clearing entity without the call amount: None, not 0.
    assert rows[2]["call_amount"] is None and rows[2]["call_currency"] is None
    assert rows[2]["put_amount"] == 35_000_000.0 and not rows[2]["notional_capped"]


def test_rows_without_an_identifier_are_dropped():
    line = ROWS.splitlines()[0]
    assert parsed(HEADER + "," + line.split(",", 1)[1] + "\n") == []


def test_source_declares_its_contract(monkeypatch):
    source = DtccFxOptions()
    assert source.table.qualified == "fx.dtcc_options"
    assert tuple(source.table.primary_key) == ("dissemination_id",)

    asked = []
    files = {DAY: tuple(records()), date(2026, 10, 2): tuple(records(HEADER + ROWS.splitlines()[0] + "\n"))}

    def fetch_day(day, asset_class):
        asked.append(asset_class)
        return files.get(day)

    monkeypatch.setattr(dtcc, "fetch_day", fetch_day)
    frame = source.fetch(Window(start=date(2026, 9, 30), end=date(2026, 10, 2)))
    # The foreign-exchange file, not the interest-rate one.
    assert set(asked) == {dtcc.FOREX}
    assert list(frame.columns) == source.table.column_names
    # The message present in both files is stored once, from the later file.
    assert len(frame) == 8 and frame["dissemination_id"].is_unique
    repeated = frame[frame["dissemination_id"] == "5574291761000000801"].iloc[0]
    assert repeated["report_date"] == date(2026, 10, 2)
    # Missing values are None, never NaN: a NaN reaches Postgres as a value.
    assert frame["settlement_date"].map(lambda v: v is None or isinstance(v, date)).all()
    assert frame["call_amount"].map(lambda v: v is None or isinstance(v, float)).all()
    assert frame["package"].map(lambda v: v is None or isinstance(v, bool)).all()

    empty = source.fetch(Window(start=date(2026, 12, 25), end=date(2026, 12, 25)))
    assert empty.empty and list(empty.columns) == source.table.column_names


def test_each_asset_class_has_its_own_file(monkeypatch):
    urls = []

    class Reply:
        status_code = 403

    monkeypatch.setattr(dtcc.requests, "get", lambda url, **_: urls.append(url) or Reply())
    dtcc.download_day.cache_clear()
    assert dtcc.fetch_day(DAY) is None and dtcc.fetch_day(DAY, dtcc.FOREX) is None
    dtcc.download_day.cache_clear()
    assert [url.rsplit("/", 1)[1] for url in urls] == [
        "CFTC_CUMULATIVE_RATES_2026_10_01.zip",
        "CFTC_CUMULATIVE_FOREX_2026_10_01.zip",
    ]


@pytest.mark.network
def test_live_file_has_euro_dollar_options_with_a_premium():
    today = datetime.now(timezone.utc).date()
    dtcc.download_day.cache_clear()
    for back in range(2, 10):  # the latest complete weekday
        day = today - timedelta(days=back)
        if day.weekday() < 5 and (rows := dtcc.fetch_day(day, dtcc.FOREX)) is not None:
            break
    else:
        pytest.fail("no DTCC foreign-exchange file in the last ten days")
    columns = DtccFxOptions().table.column_names
    options = [dict(zip(columns, row)) for row in parse_fx_options(rows, day)]
    new = [r for r in options if r["pair"] == "EUR USD" and r["action_type"] == "NEWT" and r["event_type"] == "TRAD"]
    assert len(new) > 100
    usable = [r for r in new if r["premium"] and r["strike"] and r["expiry"] and not r["notional_capped"]]
    assert len(usable) > 50
    assert all(0.5 < r["strike"] < 2.0 for r in usable if r["strike_pair"] == "EUR/USD")
