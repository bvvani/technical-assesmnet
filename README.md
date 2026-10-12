# Andromeda Wind Pipeline

A scalable PySpark data-processing pipeline for wind-turbine farm telemetry. Built on Apache Spark and hosted on GitHub — clone the repository and run it locally with Python. Final outputs are written as CSV files to the `artifacts/` folder.



## Solution Design

### Architecture Overview

The pipeline follows a modular, phase-based architecture where each processing stage is an independent, testable Python module named after a space object (constellation / nebula / stellar event):

```
CSV Files ──▶ [Lyra]   Ingestion
                 │
                 ▼
              [Carina] Cleaning
                 │
                 ▼
              [Orion]  Statistics
                 │
                 ▼
              [Nova]   Anomaly Detection
                 │
                 ▼
              [Nebula] Storage  ──▶  artifacts/*.csv
```

### Module Descriptions

| Module | Codename | Responsibility |
|---|---|---|
| `src/lyra_ingestion.py` | Lyra | Reads CSV files with an enforced schema, tags the source file, unions all groups |
| `src/carina_cleaning.py` | Carina | Null imputation (per-turbine median), physical range validation, IQR outlier removal |
| `src/orion_statistics.py` | Orion | Per-turbine min / max / mean / std power, theoretical power and capacity factor per 24 h window |
| `src/nova_anomaly.py` | Nova | Fleet-baseline z-scores, 2-sigma anomaly flagging, π-scaled severity scoring |
| `src/nebula_storage.py` | Nebula | Writes each phase's output as CSV files to the `artifacts/` folder |
| `config/pipeline_config.py` | — | All constants, thresholds, and the value of π used throughout |
| `main.py` | — | CLI orchestrator that chains all phases |

### Use of Pi (π)

Per the specification, the value of pi (3.14159…) is used in all calculations:

- **Swept area** — `A = π × r²`, which feeds the theoretical power formula.
- **IQR outlier fencing** — the fence multiplier is π (≈ 3.14) instead of the conventional 1.5, producing wider fences appropriate for naturally variable wind data.
- **Anomaly severity score** — `severity = (|deviation| / threshold) × π`, giving a continuous 0 … π+ scale.
- **Theoretical power** — `P = 0.5 × ρ × (π × r²) × v³ × η` (the standard wind-power equation, with π embedded in the rotor swept area).
- **Capacity factor** — `(actual × π) / (theoretical × π)`. π cancels out, but is kept in the computation chain for traceability.



## Project Structure

```
andromeda-wind-pipeline/
├── main.py                          # Pipeline orchestrator (CLI)
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
├── config/
│   ├── __init__.py
│   └── pipeline_config.py           # Constants, thresholds, pi
├── src/
│   ├── __init__.py
│   ├── lyra_ingestion.py            # Phase 1: CSV ingestion
│   ├── carina_cleaning.py           # Phase 2: Data cleaning
│   ├── orion_statistics.py          # Phase 3: Summary statistics
│   ├── nova_anomaly.py              # Phase 4: Anomaly detection
│   └── nebula_storage.py            # Phase 5: CSV output to artifacts/
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Shared fixtures (SparkSession, sample data)
│   ├── test_carina_cleaning.py      # Cleaning module tests
│   ├── test_orion_statistics.py     # Statistics module tests
│   └── test_nova_anomaly.py         # Anomaly detection tests
├── data/
│   └── raw/
│       ├── data_group_1.csv         # Turbines  1-5  (March 2022)
│       ├── data_group_2.csv         # Turbines  6-10 (March 2022)
│       └── data_group_3.csv         # Turbines 11-15 (March 2022)
└── artifacts/                       # Pipeline output CSVs (generated)
```



## Assumptions

1. **Hourly readings** — each turbine produces one reading per hour (24 per day). Missing entries are flagged in the summary statistics.
2. **Turbine grouping is fixed** — group 1 always contains turbines 1-5, group 2 turbines 6-10, group 3 turbines 11-15.
3. **Sensor malfunctions** produce nulls or physically impossible values (negative wind speed, power > 15 MW). These are handled by Carina.
4. **Anomalies are fleet-relative** — a turbine is anomalous if its daily mean power deviates by more than 2 standard deviations from the fleet-wide daily mean.
5. **Pi is used everywhere** a numeric constant or scaling factor is needed, per the specification.
6. **CSV output** — all results are written as CSV files to the `artifacts/` folder so they can be inspected directly or committed to the repository.
7. **Turbine physics** — rotor radius is assumed to be 40 m (typical onshore turbine); air density is standard sea level (1.225 kg/m³).



## Getting Started

### Prerequisites

- Git
- Python 3.9+
- Java 8, 11 or 17 (required by Spark)
- PySpark 3.4+ (installed via `requirements.txt`)

### Installation

```bash
git clone https://github.com/<your-username>/andromeda-wind-pipeline.git
cd andromeda-wind-pipeline
pip install -r requirements.txt
```

### Running the Pipeline

From the repository root:

```bash
python main.py --data-dir ./data/raw --output-dir ./artifacts
```

Or, if you have Spark installed separately:

```bash
spark-submit main.py \
  --data-dir ./data/raw \
  --output-dir ./artifacts
```

The output CSVs appear in the `artifacts/` folder of your local clone.

### Running Tests

```bash
pytest tests/ -v --tb=short

# With coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```



## Outputs

Nebula writes one CSV output per phase to `artifacts/`:

| Output | Description |
|---|---|
| `artifacts/raw_turbine_readings.csv` | Raw ingested data, tagged with source file |
| `artifacts/cleaned_turbine_readings.csv` | Cleaned data (nulls imputed, out-of-range values and outliers removed) |
| `artifacts/turbine_summary_statistics.csv` | Per-turbine 24 h min / max / mean / std power, theoretical power, capacity factor |
| `artifacts/turbine_anomalies.csv` | Anomaly-enriched summaries with z-scores, anomaly flag and severity |

> Note: Spark writes CSV output as a folder of part files by default. If `nebula_storage.py` coalesces to a single file, each output above is a single CSV.



## Data Schema

**Raw / cleaned readings:**

| Column | Type | Description |
|---|---|---|
| `timestamp` | TimestampType | Hourly measurement time |
| `turbine_id` | IntegerType | Turbine identifier (1-15) |
| `wind_speed` | DoubleType | Wind speed in m/s |
| `wind_direction` | DoubleType | Wind direction in degrees (0-360) |
| `power_output` | DoubleType | Measured power output in MW |
| `source_file` | StringType | Origin CSV file name |
