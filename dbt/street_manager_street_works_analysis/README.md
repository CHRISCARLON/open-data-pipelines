## Street Works Impact Scores Model (High Level Explanation)

### Overview

This model calculates and normalises impact scores for road works across England's highway network.

It integrates permit data with traffic and infrastructure metrics to produce weighted and normalised impact scores, reflecting both the direct impact of works and their significance within the broader network context.

---

### Input Data Sources

1. **Permit Data**

   - In-progress works (`in_progress_list_england`)
   - Completed works (`completed_list_england`)
   - Key fields: USRN, street name, highway authority, work category, TTRO requirements, traffic sensitivity, traffic management type

2. **Infrastructure Data**
   - UPRN-USRN mapping (`uprn_usrn_count`)
   - DfT Local Authority data (`dft_data_joins`): contains road length and traffic flow estimates (currently for 2023)

---

### Impact Score Calculation

#### 1. Base Impact Level

Each permit record is assigned a base impact level, calculated as the sum of:

- **Work Category Impact** (1–5 points)

  - Major works: 5 points
  - Immediate works: 4 points
  - Standard works: 2 points
  - Minor works: 1 point
  - HS2 (Highway): 2 points
  - Other/unknown: 0 points

- **Additional Impact Factors**
  - TTRO Required: +0.5 points
  - Traffic Sensitive: +0.5 points
  - Traffic Management Impact:
    - High impact (e.g., road closure, lane closure, signals): +2.0 points
    - Medium impact (e.g., give and take, stop/go boards): +1.0 point
    - Low impact (some carriageway incursion): +0.5 points
    - No impact or unknown: 0–0.5 points
  - UPRN Density Impact (based on property density on the street): +0.2 to +1.6 points

#### 2. Aggregation

- Impact levels are summed for each unique street (USRN) to produce a `total_impact_level`.

---

### Network Context Adjustment

#### 3. Network Importance Factor

- For each local authority, a **network importance factor** is calculated based on traffic density:
  - `traffic_density = traffic_flow_2023 / total_road_length`
- This value is normalised to a 0–1 scale by dividing each authority's traffic density by the maximum observed across all authorities:
  - `network_importance_factor = traffic_density / max(traffic_density)`

#### 4. Weighted Impact Level

- The raw impact level for each street is multiplied by (1 + network importance factor):
  - `weighted_impact_level = total_impact_level * (1 + network_importance_factor)`
- This amplifies the impact of works in more critical (higher-density) networks, reflecting their greater potential for disruption.

---

### Normalization and Final Impact Index Calculation

#### 5. Min-Max Normalization

- The weighted impact levels are normalised across all records using min-max normalization:
  ```
  impact_index_score = 1 + 99 * (weighted_impact_level - min) / (max - min)
  ```
- This scales all scores to a 1–100 range, where 1 represents the lowest observed impact and 100 the highest.
- If all weighted impact levels are identical, a default score of 50 is assigned to all records.

#### 6. Categorization

- The normalised impact index score is mapped to a categorical label for easier interpretation:
  - 80–100: Critical
  - 60–79: High
  - 40–59: Medium
  - 20–39: Low
  - 1–19: Minimal

---

### How the Normalized Values Interact

- The **network importance factor** acts as a multiplier, increasing the impact of works in more critical areas before the final normalisation.
- The **min-max normalisation** ensures that the final index is relative, highlighting the most and least impactful works in the current dataset.
- This two-stage normalisation process ensures that both the local context (street-level impact) and the broader network context (authority-level importance) are reflected in the final impact index score.

---

### Output

The final model produces a table with:

- Location identifiers (USRN, street name, highway authority)
- UPRN count
- Raw and weighted impact scores
- Network metrics (road length, traffic flow, density, network importance factor)
- Normalised impact index score (1–100)
- Impact category (Critical, High, Medium, Low, Minimal)
- Processing timestamp
