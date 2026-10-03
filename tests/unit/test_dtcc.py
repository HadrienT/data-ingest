"""dtcc-swaptions and dtcc-swap-rates: the parsers on rows recorded from the
real file of 2026-09-30 (columns trimmed to the ones read), and the sources'
contract. The `network` test at the bottom downloads a real day."""

import csv
import io
import zipfile
from datetime import date, datetime, timedelta, timezone

import pytest

from data_ingest.core.source import Window
from data_ingest.sources import _dtcc_common as dtcc
from data_ingest.sources.dtcc_swap_rates import DtccSwapRates, parse_swap_rates
from data_ingest.sources.dtcc_swaptions import DtccSwaptions, parse_swaptions

DAY = date(2026, 9, 30)

HEADER = (
    "Dissemination Identifier,Original Dissemination Identifier,Action type,Event type,Execution Timestamp,"
    "Effective Date,Expiration Date,Maturity date of the underlier,First exercise date,Notional amount-Leg 1,"
    "Notional currency-Leg 1,Fixed rate-Leg 1,Fixed rate-Leg 2,Option Premium Amount,Option Premium Currency,"
    "Strike Price,Strike price notation,Other payment amount,Non-standardized term indicator,Platform identifier,"
    "Package indicator,Cleared,UPI FISN,UPI Underlier Name\n"
)

# A straddle traded on a platform (both legs carry the same premium), a
# bilateral trade, a capped notional, a novation, a correction, a EUR
# swaption; then two options that are not swaptions.
OPTIONS = """\
5564974020000000101,,NEWT,TRAD,2026-09-30T20:32:32Z,2026-09-30,2028-09-29,2033-10-03,2028-09-29,"50,000,000",USD,0.04891,,"2,600,000",USD,0.04891,3,,FALSE,ISWV,TRUE,N,NA/O Call Epn OIS USD,NA/Swap OIS USD
5564974021000000201,,NEWT,TRAD,2026-09-30T20:32:32Z,2026-09-30,2028-09-29,2033-10-03,2028-09-29,"50,000,000",USD,0.04891,,"2,600,000",USD,0.04891,3,,FALSE,ISWV,TRUE,N,NA/O P Epn OIS USD,NA/Swap OIS USD
5562842814000000501,,NEWT,TRAD,2026-09-30T19:03:42Z,2026-09-30,2027-03-03,2037-03-05,2027-03-03,"39,000,000",USD,0.0544,,"299,393.64",USD,0.0544,3,,FALSE,BILT,FALSE,N,NA/O Call Epn OIS USD,NA/Swap OIS USD
5560927407000000101,,NEWT,TRAD,2026-09-30T16:24:05Z,2026-09-30,2036-08-11,2046-08-13,2036-08-11,"250,000,000+",USD,0.049,,"9,768,750",USD,0.049,3,,TRUE,BILT,TRUE,N,NA/O Call Epn OIS USD,NA/Swap OIS USD
5564366924000000301,,NEWT,NOVA,2026-09-30T19:28:34Z,2026-09-23,2026-12-23,2036-12-28,2026-12-23,"60,000,000",USD,0.0463,,0,,0.0463,3,,FALSE,BILT,FALSE,N,NA/O Call Epn OIS USD,NA/Swap OIS USD
5560270543000000101,5558846636000000101,CORR,,2026-09-30T15:51:12Z,2026-09-30,2028-09-29,2038-10-03,2028-09-29,"250,000,000",USD,0.0501,,"9,987,513.04756",USD,0.0501,3,,TRUE,BILT,FALSE,N,NA/O P Epn OIS USD,NA/Swap OIS USD
5546013547000000401,,NEWT,TRAD,2026-09-30T07:33:32Z,2026-09-30,2026-10-30,2036-11-03,2026-10-30,"25,000,000",EUR,0.03666,,"437,500",EUR,0.03666,3,,FALSE,BILT,FALSE,N,NA/O Opt Epn Fxd Flt EUR,NA/Swap Fxd Flt EUR
5566806082000000101,,NEWT,TRAD,2026-09-30T19:58:54Z,2026-09-30,2027-10-01,,2027-10-01,"17,000,000",USD,,,"105,605.75",USD,0.04,3,,TRUE,BILT,FALSE,N,NA/O Call Epn USD,USD-SOFR CME Term
5541812877000000101,3458677535000000201,MODI,TRAD,2026-06-02T16:20:57Z,2026-06-02,2036-06-02,,2026-12-04,"10,000,000",USD,0.0276,,0,USD,0.0276,3,,TRUE,BILT,TRUE,N,NA/O Nstd Oth USD,USD-SOFR-OIS Compound
"""

# Four spot-starting five-year swaps; a fifth with an upfront payment; a
# forward-starting one; two ten-year swaps (one short of the minimum); a
# termination; a EUR swap.
SWAPS = """\
5548035814000000501,,NEWT,TRAD,2026-09-30T08:13:59Z,2026-10-02,2031-10-02,,,"30,000",USD,0.04749,,,,,,,,TWSF,FALSE,I,NA/Swap OIS USD,USD-SOFR-COMPOUND
5541816303000000201,,NEWT,TRAD,2026-09-30T02:38:04Z,2026-10-02,2031-10-02,,,"34,000,000",USD,0.047689,,,,,,,FALSE,BBSF,TRUE,I,NA/Swap OIS USD,USD-SOFR-COMPOUND
5564749377000000101,,NEWT,TRAD,2026-09-30T20:23:44Z,2026-10-02,2031-10-02,,,"100,000,000",USD,0.0481556,,,,,,,FALSE,BGCD,TRUE,I,NA/Swap OIS USD,USD-SOFR-OIS Compound
5564765103000000101,,NEWT,TRAD,2026-09-30T20:24:29Z,2026-10-02,2031-10-02,,,"200,000,000",USD,0.0481556,,,,,,,FALSE,BGCD,TRUE,I,NA/Swap OIS USD,USD-SOFR-OIS Compound
5561248077000000201,,NEWT,TRAD,2026-09-30T17:41:51Z,2026-10-02,2031-10-02,,,"5,000,000",USD,0.048215,,,,,,810.00000,,TWSF,TRUE,I,NA/Swap OIS USD,USD-SOFR-COMPOUND
5541731315000000301,,NEWT,TRAD,2026-09-30T01:58:27Z,2029-05-31,2059-05-31,,,"9,000,000",USD,0.0492,,,,,,61320.50148,,TWSF,TRUE,I,NA/Swap OIS USD,USD-SOFR-OIS Compound
5547908688000000101,,NEWT,TRAD,2026-09-30T08:11:26Z,2026-10-02,2036-10-02,,,"32,000,000",USD,0.048265,,,,,,,,TWSF,FALSE,I,NA/Swap OIS USD,USD-SOFR-COMPOUND
5541856645000000101,,NEWT,TRAD,2026-09-30T03:06:21Z,2026-10-02,2036-10-02,,,"20,000,000",USD,0.048467,,,,,,,,TWSF,FALSE,I,NA/Swap OIS USD,USD-SOFR-OIS Compound
5565845277000000101,400301711,TERM,ETRM,2022-08-04T17:42:45Z,2022-08-15,2029-08-15,,,"470,000,000+",USD,0.0254,,,,,,,TRUE,BILT,FALSE,N,NA/Swap OIS USD,USD-SOFR-COMPOUND
5544482873000000501,,NEWT,TRAD,2026-09-30T07:10:08Z,2026-11-04,2026-12-23,,,"1,300,000,000",EUR,0.025201,,,,,,12996.99664,,TWSF,FALSE,I,NA/Swap OIS EUR,EUR-EuroSTR-OIS Compound
"""


def records(text=HEADER + OPTIONS + SWAPS):
    return list(csv.DictReader(io.StringIO(text)))


def as_dicts(source, rows):
    return [dict(zip(source.table.column_names, row)) for row in rows]


def zipped(text, bom=False):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CFTC_CUMULATIVE_RATES_2026_09_30.csv", ("﻿" if bom else "") + text)
    return buffer.getvalue()


# ── shared helpers ───────────────────────────────────────────────────────────


def test_numbers_drop_separators_and_the_cap_marker_and_never_return_nan():
    assert dtcc.number("250,000,000+") == 250_000_000.0
    assert dtcc.number("9,987,513.04756") == 9_987_513.04756
    assert dtcc.number("0") == 0.0
    assert dtcc.number("") is None and dtcc.number(None) is None
    assert dtcc.number("NaN") is None and dtcc.number("n/a") is None
    assert dtcc.is_capped("250,000,000+") and not dtcc.is_capped("250,000,000")


def test_dates_timestamps_and_booleans():
    assert dtcc.iso_date("2028-09-29") == date(2028, 9, 29)
    assert dtcc.iso_date("") is None and dtcc.iso_date("29/09/2028") is None
    assert dtcc.timestamp("2026-09-30T20:32:32Z") == datetime(2026, 9, 30, 20, 32, 32, tzinfo=timezone.utc)
    assert dtcc.timestamp("") is None
    assert dtcc.boolean("TRUE") is True and dtcc.boolean("FALSE") is False and dtcc.boolean("") is None


def test_zip_with_a_byte_order_mark_keeps_the_first_column_name():
    rows = dtcc.read_rates_zip(zipped(HEADER + OPTIONS, bom=True))
    assert rows[0]["Dissemination Identifier"] == "5564974020000000101"


def test_window_days_are_calendar_days_and_a_full_run_reaches_two_years_back():
    assert list(dtcc.window_days(Window(start=date(2026, 9, 25), end=date(2026, 9, 28)))) == [
        date(2026, 9, 25),
        date(2026, 9, 26),  # a Saturday: weekend files exist
        date(2026, 9, 27),
        date(2026, 9, 28),
    ]
    days = list(dtcc.window_days(Window.everything()))
    today = datetime.now(timezone.utc).date()
    assert days[0] == today - timedelta(days=dtcc.FULL_HISTORY_DAYS) and days[-1] == today
    # Never past today, whatever the window says.
    assert list(dtcc.window_days(Window(start=today, end=today + timedelta(days=3)))) == [today]


def test_a_day_without_a_downloadable_file_is_none_not_an_error(monkeypatch):
    class Reply:
        def __init__(self, status, content=b""):
            self.status_code, self.content = status, content

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(self.status_code)

    replies = {date(2024, 1, 2): Reply(403), DAY: Reply(200, zipped(HEADER + SWAPS)), date(2026, 9, 29): Reply(500)}
    monkeypatch.setattr(dtcc.requests, "get", lambda url, **_: replies[date(*map(int, url[-14:-4].split("_")))])
    dtcc.download_day.cache_clear()
    assert dtcc.fetch_day(date(2024, 1, 2)) is None  # cold storage answers 403
    assert len(dtcc.fetch_day(DAY)) == 10
    with pytest.raises(RuntimeError):  # a real server error is not swallowed
        dtcc.fetch_day(date(2026, 9, 29))
    dtcc.download_day.cache_clear()


# ── dtcc-swaptions ───────────────────────────────────────────────────────────


def test_swaptions_keep_every_message_on_a_swap_and_nothing_else():
    rows = as_dicts(DtccSwaptions(), parse_swaptions(records(), DAY))
    assert len(rows) == 7
    # The option on Term SOFR and the non-standard option are not swaptions;
    # swaps are not options.
    assert {r["underlier"] for r in rows} == {"NA/Swap OIS USD", "NA/Swap Fxd Flt EUR"}
    assert [r["option_type"] for r in rows] == ["call", "put", "call", "call", "call", "put", "other"]
    assert all(r["report_date"] == DAY for r in rows)


def test_swaption_fields_are_stored_as_published():
    rows = {r["dissemination_id"]: r for r in as_dicts(DtccSwaptions(), parse_swaptions(records(), DAY))}

    bilateral = rows["5562842814000000501"]
    assert bilateral["currency"] == "USD" and bilateral["product"] == "NA/O Call Epn OIS USD"
    assert bilateral["expiry"] == date(2027, 3, 3) and bilateral["underlier_maturity"] == date(2037, 3, 5)
    assert bilateral["strike"] == 0.0544 and bilateral["strike_notation"] == "3"
    assert bilateral["notional"] == 39_000_000.0 and bilateral["notional_capped"] is False
    assert bilateral["premium"] == 299_393.64 and bilateral["premium_currency"] == "USD"
    assert bilateral["platform"] == "BILT" and bilateral["package"] is False and bilateral["cleared"] == "N"
    assert bilateral["execution_timestamp"] == datetime(2026, 9, 30, 19, 3, 42, tzinfo=timezone.utc)
    assert (bilateral["action_type"], bilateral["event_type"]) == ("NEWT", "TRAD")

    capped = rows["5560927407000000101"]
    assert capped["notional"] == 250_000_000.0 and capped["notional_capped"] is True
    assert capped["premium"] == 9_768_750.0  # the premium is not capped

    novation = rows["5564366924000000301"]
    assert novation["event_type"] == "NOVA" and novation["premium"] == 0.0
    assert novation["premium_currency"] is None  # empty stays NULL, not ""

    correction = rows["5560270543000000101"]
    assert correction["action_type"] == "CORR" and correction["event_type"] is None
    assert correction["original_dissemination_id"] == "5558846636000000101"


def test_a_platform_straddle_is_two_rows_with_the_same_premium():
    # The rule the consumer applies (halve the premium) rests on this shape.
    rows = as_dicts(DtccSwaptions(), parse_swaptions(records(), DAY))
    call, put = rows[0], rows[1]
    same = ("expiry", "underlier_maturity", "strike", "notional", "premium", "platform")
    assert all(call[k] == put[k] for k in same)
    assert {call["option_type"], put["option_type"]} == {"call", "put"}
    assert call["platform"] == "ISWV" and call["package"] is True


def test_swaption_rows_without_a_key_or_a_currency_are_dropped():
    broken = OPTIONS.splitlines()[2].split(",")
    no_id = ",".join([""] + broken[1:])
    assert parse_swaptions(records(HEADER + no_id + "\n"), DAY) == []
    assert len(parse_swaptions(records(HEADER + OPTIONS.splitlines()[2] + "\n"), DAY)) == 1


def test_swaptions_source_declares_its_contract(monkeypatch):
    source = DtccSwaptions()
    assert source.table.qualified == "rates.dtcc_swaptions"
    assert tuple(source.table.primary_key) == ("dissemination_id",)

    files = {DAY: tuple(records()), date(2026, 10, 1): tuple(records(HEADER + OPTIONS.splitlines()[2] + "\n"))}
    monkeypatch.setattr(dtcc, "fetch_day", lambda day: files.get(day))
    frame = source.fetch(Window(start=date(2026, 9, 29), end=date(2026, 10, 1)))
    assert list(frame.columns) == source.table.column_names
    # The message present in both files is stored once, from the later file.
    assert len(frame) == 7 and frame["dissemination_id"].is_unique
    repeated = frame[frame["dissemination_id"] == "5562842814000000501"].iloc[0]
    assert repeated["report_date"] == date(2026, 10, 1)
    # Missing values are None, never NaN: a NaN reaches Postgres as a value.
    assert frame["underlier_maturity"].map(lambda v: v is None or isinstance(v, date)).all()
    assert frame["package"].map(lambda v: v is None or isinstance(v, bool)).all()

    # No file at all in the window (a holiday, the future): an empty frame.
    empty = source.fetch(Window(start=date(2026, 12, 25), end=date(2026, 12, 25)))
    assert empty.empty and list(empty.columns) == source.table.column_names


# ── dtcc-swap-rates ──────────────────────────────────────────────────────────


def test_par_rates_are_the_median_of_the_spot_starting_at_market_swaps():
    rows = parse_swap_rates(records(), DAY)
    # Five years: four trades. The one with an upfront payment, the
    # forward-starting one, the termination and the EUR swap are left out; the
    # ten-year point has two trades, below the minimum.
    assert len(rows) == 1
    day, product, currency, tenor, rate, low, high, trades = rows[0]
    assert (day, product, currency, tenor, trades) == (DAY, "NA/Swap OIS USD", "USD", 5, 4)
    assert rate == pytest.approx((0.047689 + 0.0481556) / 2)
    assert low == pytest.approx(0.04749 + 0.75 * (0.047689 - 0.04749))
    assert high == pytest.approx(0.0481556)
    assert low <= rate <= high


def test_par_rates_need_enough_trades_and_a_plausible_rate():
    ten_years = SWAPS.splitlines()[6]
    three = "\n".join([ten_years] * 3) + "\n"
    rows = parse_swap_rates(records(HEADER + three), DAY)
    assert [(r[3], r[7]) for r in rows] == [(10, 3)]
    # A rate published as a percentage (4.8265 for 0.048265) is not averaged in.
    wrong_unit = three.replace("0.048265", "4.8265")
    assert parse_swap_rates(records(HEADER + wrong_unit), DAY) == []
    # A maturity that is not a whole number of years fits no tenor.
    stub = three.replace("2036-10-02", "2036-04-02")
    assert parse_swap_rates(records(HEADER + stub), DAY) == []


def test_the_euro_has_an_estr_curve_and_a_euribor_one():
    five_years = SWAPS.splitlines()[:4]
    estr = "\n".join(five_years).replace("NA/Swap OIS USD", "NA/Swap OIS EUR").replace(",USD,", ",EUR,")
    euribor = estr.replace("NA/Swap OIS EUR", "NA/Swap Fxd Flt EUR")
    rows = parse_swap_rates(records(HEADER + SWAPS + estr + "\n" + euribor + "\n"), DAY)
    assert [(r[1], r[2], r[3], r[7]) for r in rows] == [
        ("NA/Swap Fxd Flt EUR", "EUR", 5, 4),
        ("NA/Swap OIS EUR", "EUR", 5, 4),
        ("NA/Swap OIS USD", "USD", 5, 4),
    ]


def test_swap_rates_source_declares_its_contract(monkeypatch):
    source = DtccSwapRates()
    assert source.table.qualified == "rates.dtcc_swap_rates"
    assert tuple(source.table.primary_key) == ("date", "product", "tenor_years")

    monkeypatch.setattr(dtcc, "fetch_day", lambda day: tuple(records()) if day == DAY else None)
    frame = source.fetch(Window(start=date(2026, 9, 29), end=DAY))
    assert list(frame.columns) == source.table.column_names
    assert len(frame) == 1 and frame["tenor_years"].iloc[0] == 5 and frame["trades"].iloc[0] == 4
    assert frame["rate"].notna().all()


# ── live ─────────────────────────────────────────────────────────────────────


@pytest.mark.network
def test_live_file_has_swaptions_and_a_sofr_curve():
    today = datetime.now(timezone.utc).date()
    dtcc.download_day.cache_clear()
    for back in range(2, 10):  # the latest complete weekday
        day = today - timedelta(days=back)
        if day.weekday() < 5 and (rows := dtcc.fetch_day(day)) is not None:
            break
    else:
        pytest.fail("no DTCC rates file in the last ten days")
    swaptions = as_dicts(DtccSwaptions(), parse_swaptions(rows, day))
    sofr = [r for r in swaptions if r["underlier"] == "NA/Swap OIS USD"]
    assert len(sofr) > 100
    assert sum(1 for r in sofr if r["premium"] and r["strike"] and r["underlier_maturity"]) > 50
    curve = {r[3]: r[4] for r in parse_swap_rates(rows, day)}
    assert {2, 5, 10, 30} <= set(curve)
    assert all(0.0 < rate < 0.15 for rate in curve.values())
