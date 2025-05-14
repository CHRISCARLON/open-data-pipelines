# Reminder of what needs to be done


## Impact Scores Model

## Overview

This model calculates and normalizes impact scores for road works across England's highway network. It combines permit data with traffic and infrastructure metrics to produce weighted impact scores that reflect both the direct impact of works and the broader network context.

## Input Data Sources

1. **Permit Data**

   - In-progress works (`in_progress_list_england`)
   - Completed works (`completed_list_england`)
   - Key fields: USRN, street name, highway authority, work category, TTRO requirements, traffic sensitivity, traffic management type

2. **Infrastructure Data**
   - UPRN-USRN mapping (`uprn_usrn_count`)
   - DFT Local Authority data (`dft_la_data_latest`) contains road length and traffic flow information

## Impact Score Calculation

### Base Impact Factors

- **Work Category Impact** (0-5 points)

  - Major works: 5 points
  - Immediate works: 4 points
  - Standard works: 2 points
  - Minor works: 1 point
  - etc

- **Additional Impact Factors**
  - TTRO Required: +0.5 points
  - Traffic Sensitive: +0.5 points
  - Traffic Management Impact: +0-2 points based on severity
  - UPRN Density Impact: +0.2-1.6 points based on UPRN point density on a USRN

### Network Context Adjustment

The model applies a network importance factor based on:

- Total road length in the authority
- Traffic flow data (2023)
- Traffic density per km (length/flow)
- Normalised network importance factor (0-1 scale)

## Output

The final model produces a table with:

- Location identifiers (USRN, street name, highway authority)
- Raw impact scores
- Network metrics (road length, traffic flow, density)
- Final weighted impact scores that account for both direct works impact and network importance

This model helps identify high-impact works areas by considering both the immediate disruption of works and their context within the broader road network.
