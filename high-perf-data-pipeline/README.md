# ⚡ High-Performance ETL Pipeline (9000x Speedup)

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Polars](https://img.shields.io/badge/Polars-Rust%20Engine-orange)
![Status](https://img.shields.io/badge/Maintained-Yes-green)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

> **Business Context:** Modernization of a legacy data cleaning pipeline.
> **Result:** Processing time reduced from **45 minutes** to **< 300 milliseconds**.

---

## 📉 The Challenge

A fictional client in the e-commerce sector faced a critical bottleneck: their daily competitor price analysis script (based on standard Python/Pandas loops) took nearly **45 minutes** to process 500,000 rows of dirty data. This delay impacted their dynamic pricing strategy every morning.

**Key Issues:**
* **Slow execution:** $O(N^2)$ complexity on product matching.
* **Dirty Data:** Inconsistent product names (typos, case sensitivity), mixed currencies ($/€), and scaling errors.
* **Memory usage:** The legacy script crashed on larger datasets.

## 🚀 The Solution

I engineered a high-performance ETL pipeline using **Polars**, a Rust-based DataFrame library, to leverage modern CPU architecture (SIMD & Parallelization).

### Technical Highlights:
* **Lazy Evaluation:** The pipeline builds a query plan and executes it only when needed, optimizing memory allocation (Streaming API).
* **Vectorized String Operations:** Replaced Python loops with native Rust string manipulation for regex and cleaning.
* **Smart Aggregation (Fingerprinting):** Implemented a normalization logic to group products despite typos and formatting errors without complex ML overhead.

## 📊 Performance Benchmark

Processing **500,000 rows** of complex dirty data on a standard machine:

| Metric | Legacy Approach | **My Solution** | Improvement |
| :--- | :--- | :--- | :--- |
| **Execution Time** | ~45 min | **273 ms** | **~9,900x Faster** |
| **Memory Footprint** | High (Load all) | **Low (Streaming)** | Optimized |

![Benchmark Screenshot](./assets/benchmark_result.png)
*(Measured using Hyperfine)*

---

## 🛠️ Code Architecture

This project follows professional software engineering practices:

```text
high-perf-data-pipeline/
├── src/
│   ├── pipeline.py       # Core ETL logic (Pure functions)
├── tests/
│   ├── test_pipeline.py  # Unit tests (Pytest) covering edge cases
├── generate_data.py      # Script to generate dirty datasets for stress testing
└── requirements.txt      # Minimal dependencies
```

## 💻 How to Run

**1. Install dependencies**
```bash
pip install -r requirements.txt
```
**2. Generate the dummy dataset (500k rows)**
```bash
python generate_data.py
```
**Run the Pipeline**
```bash
# Usage: python src/pipeline.py -i [INPUT] -o [OUTPUT]
python src/pipeline.py -i dirty_catalogue.csv -o clean_catalogue.csv
```

---

## 👤 Author

**Sacha Metzger**
* **Focus:** High-Performance Python, Automation, Data Engineering.
* **Background:** Competitive Programming (Top 3 France Algo).

*Open for freelance opportunities involving complex logic & optimization.*
