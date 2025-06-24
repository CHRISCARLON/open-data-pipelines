# Open Data Pipelines 📊

Data pipelines for working with UK open data sources.

## Purpose

This project enables data analysts to perform analysis on UK open data by automating the extraction, loading, and transformation (ELT) of data from various government and public sector sources.

## Current Data Sources

- **Street Manager** - Street Works data for England
- **Ordnance Survey** - USRNs, Linked Identifiers
- **Geoplace SWA Codes** - Street Works Authority codes
- **NAPTAN** - Public Transport Access Nodes
- **BDUK Premises Data** - Project Gigabit premises
- **Built Up Areas** - OS Built Up Areas boundaries
- **NHS Prescribing Data** - English prescribing data
- **Utility Company Open Data** - Underground infrastructure

## Current Analyses

DBT models are set up for certain analyses:

- **Street Works Impact Index Score** - this feeds into [Word on the Street](https://word-on-the-street.evidence.app)
- **NAPTAN to USRNs** - this is a test of the NAPTAN data against the USRN data - can we geopstically join them??

## License

This project is licensed under the MIT License - see the LICENSE file for details.
