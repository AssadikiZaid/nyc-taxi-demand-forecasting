# Data notes

Facts about the TLC yellow-taxi data found while building the panel, to keep in mind when
exploring the data and modeling (phase 2 onwards). Cleaning decisions are in
[decisions.md](decisions.md); monthly counts are in [data_quality.md](data_quality.md).

## Observed on January 2023 to August 2026

- **Timestamps are New York wall-clock time.** In all 44 months, pickups peak at 18:00 and
  bottom out at 04:00 or 05:00.
- **Vendor 7 never records a dropoff time** (dropoff = pickup on every trip). Its share
  grows from 0.03 % of trips in January 2025 to about 1.3 % in May 2026.
- **Flex-fare trips** (payment type 0) often have no recorded distance and, until at least
  November 2025, often a negative `fare_amount` with a positive total. In May 2026,
  negative fares fall to 0.35 % of trips (5.0 % in March 2025): the encoding changed, not
  the trips.
- **Reversal records** (a trip repeated with the opposite fare) peak at 1.9 % of rows in
  August 2025 and drop to about 0.3 % from April 2026, along with fewer dispute and
  no-charge records.
- **Unknown zones** (264-265) fall from 1.4 % of trips in January 2023 to about 0.2 % in
  2026.
- **Structural change in June 2026 outside Manhattan.** Vendor 1's share of pickups outside
  Manhattan and the airports falls from 15.2 % in May 2026 to 3.8 % in June, and its share
  of all trips from 19.7 % to 16.0 %. Zones with at least 100 pickups a month fall from 226
  to 211, and the share of zone-hours without any pickup rises from 39.5 % to 49.6 %. Per
  day, pickups fall by 22.5 % outside the 50 busiest zones of May but only by 0.9 % in
  those zones, which hold 90 % of the trips. Whether these trips moved to
  other services or stopped being reported is unknown; the test period of the backtest
  includes this change.
