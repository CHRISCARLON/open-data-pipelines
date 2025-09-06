# Open Data Pipelines!

Simple data pipelines for working with UK open data sources.

This project mainly uses MotherDuck as its data warehouse but it is configured to run with Postgres as well.

> [!NOTE]
> Currently switching most of the data pipelines from AWS to run in Github Actions - this is a work in progress but massively reduces complexity and cost.

> [!NOTE]
> Currently looking to implement DLT into this project - some pipelines will start to use & refernce DLT methods as a result.

## Purpose

This project enables data analysts to perform analysis on UK open data by automating the extraction, loading, and transformation (ELT) of data from various government and public sector sources.

Some pipelines are more advanced than others.

## Current Data Sources

- **Street Manager** - Permit data and Section 58 data for England
- **Geoplace SWA Codes** - Street Works Authority codes
- **National Public Transport Access Nodes (NaPTAN)** - Public Transport Access Nodes
- **BDUK Premises Data** - Project Gigabit premises
- **Built Up Areas** - OS Built Up Areas boundaries
- **NHS Prescribing Data** - English prescribing data
- **Cadent Gas Asset Network** - Underground infrastructure
- **Open Bus Data BODS** - Bus and coach timetable data in GTFS format
- **Ordnance Survey** - Linked Identifiers, Unique Street Reference Numbers (USRN), Unique Property Reference Numbers (UPRN), Code Point Open Data
- **Census 2021 Postcode P001** - P001: Number of usual residents by postcode by sex
- **Census 2021 Postcode P002** - P002: Number of households by postcode
- **National Statistics Postcode Lookup** - The NSPL relates both current and terminated postcodes to a range of current statutory geographies via ‘best-fit’ allocation from the 2021 Census Output Areas (national parks and Workplace Zones are exempt from ‘best-fit’ and use ‘exact-fit’ allocations) for England, Wales, Scotland and Northern Ireland.

## Current Analyses

Some DBT models are set up for more complex analysis:

- **Street Works Impact Index Score** - this feeds into [Word on the Street](https://word-on-the-street.evidence.app)

## Github Actions

Github Actions are used to run the data pipelines with cron schedules.

## License

This project is licensed under the MIT License - see the LICENSE file for details!
