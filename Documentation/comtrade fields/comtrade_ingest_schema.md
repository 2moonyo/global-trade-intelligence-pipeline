# UN Comtrade Ingestion Schema Documentation

This document describes the core fields extracted during the UN Comtrade
ingestion stage of the data pipeline. The fields are grouped by their
logical purpose in the dataset.

The dataset represents international trade transactions reported by
customs agencies to the United Nations Comtrade system.

# 1. Temporal (Time) Fields

These fields define when the trade occurred.

  -----------------------------------------------------------------------
  Field Name              Data Type             Description
  ----------------------- --------------------- -------------------------
  refYear                 Integer               The four digit year in
                                                which the trade occurred.
                                                Example: 2024.

  refMonth                Integer               The numeric month of the
                                                trade record. Example: 5
                                                for May.

  period                  Integer / String      Concatenated year and
                                                month used for batching
                                                and indexing. Example:
                                                202405. In the Silver
                                                layer this should be cast
                                                to a String so it can
                                                easily be converted into
                                                a DateTime object for
                                                time series analysis.
  -----------------------------------------------------------------------

# 2. Geographic and Directional Fields

These fields define who is trading and where the goods are moving.

  -----------------------------------------------------------------------
  Field Name              Data Type             Description
  ----------------------- --------------------- -------------------------
  flowCode / flowDesc     String                Indicates the direction
                                                of trade. X represents
                                                Export, M represents
                                                Import.

  reporterCode /          Integer / String      The country reporting the
  reporterISO                                   data to the UN Comtrade
                                                system. Example: 97 / EUR
                                                for the European Union.

  reporterDesc            String                Human readable name of
                                                the reporting country or
                                                economic bloc. Example:
                                                European Union.

  partnerCode /           Integer / String      The primary trading
  partnerISO                                    partner. Example: 156 /
                                                CHN. For imports this
                                                represents the Country of
                                                Origin. For exports this
                                                represents the Country of
                                                Destination.

  partnerDesc             String                Human readable name of
                                                the partner country.
                                                Example: China.

  partner2Code /          Integer / String      Secondary partner
  partner2ISO                                   (Country of Consignment).
                                                In this project this will
                                                typically be 0 / W00
                                                representing World
                                                because the extraction
                                                process standardised this
                                                field to avoid double
                                                counting.
  -----------------------------------------------------------------------

# 3. Product (Commodity) Fields

These fields define what physical commodity is being traded.

  -----------------------------------------------------------------------
  Field Name              Data Type             Description
  ----------------------- --------------------- -------------------------
  cmdCode                 String                Harmonized System (HS)
                                                commodity code. This must
                                                remain a String so that
                                                leading zeros are
                                                preserved. Example: 0901,
                                                1001, 2709.

  cmdDesc                 String                Official description of
                                                the commodity. Example:
                                                Wheat and meslin or
                                                Petroleum oils and oils
                                                obtained from bituminous
                                                minerals; crude.

  aggrLevel               Integer               Hierarchical level of the
                                                HS classification.
                                                Because the ingestion
                                                query fixes the HS level,
                                                this should consistently
                                                be 4 digit HS codes
                                                across the dataset.
  -----------------------------------------------------------------------

# 4. Quantitative (Metrics) Fields

These fields represent the core numerical metrics used for analysis.

  -----------------------------------------------------------------------
  Field Name              Data Type             Description
  ----------------------- --------------------- -------------------------
  primaryValue            Float                 Total economic value of
                                                the trade shipment
                                                expressed in United
                                                States Dollars (USD).

  netWgt                  Float                 Physical shipment weight
                                                measured in kilograms
                                                (kg).

  qty                     Float                 Quantity of goods
                                                measured using the unit
                                                defined in qtyUnitCode.

  qtyUnitAbbr             String                Abbreviation of the
                                                quantity unit used in qty
                                                such as kg for kilograms
                                                or l for litres.
  -----------------------------------------------------------------------

# 5. Metadata and Flags

These fields provide quality information about the dataset.

  -----------------------------------------------------------------------
  Field Name              Data Type             Description
  ----------------------- --------------------- -------------------------
  isNetWgtEstimated       Boolean               True when the reporting
                                                country did not provide
                                                weight information and
                                                the UN estimated the
                                                value.

  motDesc                 String                Mode of transport such as
                                                Sea or Air. Many
                                                countries do not reliably
                                                report this field.

  isReported              Boolean               True when the data was
                                                reported by customs
                                                authorities. False when
                                                the UN estimated it using
                                                mirror statistics.
  -----------------------------------------------------------------------

# Data Engineering Notes

## Key Type Rules

  -----------------------------------------------------------------------
  Field                   Recommended Type        Reason
  ----------------------- ----------------------- -----------------------
  cmdCode                 String                  Preserves leading zeros
                                                  in HS codes

  period                  String                  Enables parsing into
                                                  timestamp fields

  reporterISO /           String                  Easier joins with ISO
  partnerISO                                      reference tables
  -----------------------------------------------------------------------

## Suggested Derived Fields in Silver Layer

  Derived Field           Description
  ----------------------- -------------------------------------
  trade_date              Parsed timestamp from period
  unit_price_usd_per_kg   primaryValue / netWgt
  trade_direction         Standardised import or export label
  reporter_region         Join with ISO country reference
  partner_region          Join with ISO country reference

# Analytical Purpose

Within this project the Comtrade dataset is used to analyse trade flows,
detect disruptions at maritime chokepoints, and derive price indicators
for downstream analytics dashboards.
