# POC — Production-Grade Data Engineering Proof of Concept

A **production-inspired Data Engineering Proof of Concept** demonstrating how to design, orchestrate, and run scalable batch data pipelines using **Apache Spark, Apache Airflow, Scala, Python, and Docker**.

This repository is intentionally structured to reflect **real-world data engineering practices** rather than toy examples. It focuses on **orchestration, modular Spark job design, environment reproducibility, and operational clarity** — the same concerns faced in production data platforms.

---

## Why This Project Exists (Hiring Manager Context)

This POC was built to answer a simple but critical question:

> *Can this engineer design, run, and reason about real data pipelines — not just write Spark code?*

What this project demonstrates:

* Strong understanding of **batch data pipeline architecture**
* Clear separation of **orchestration vs compute**
* Comfort with **Spark internals, packaging, and deployment**
* Practical use of **Airflow for scheduling and dependency management**
* Production-minded use of **Docker for reproducibility**
* Clean, extensible repository structure suitable for scaling

---

## Core Technologies

* **Apache Spark (Scala)** — Distributed batch processing
* **Apache Airflow (Python)** — Workflow orchestration
* **Docker & Docker Compose** — Environment isolation and reproducibility
* **SBT** — Scala build and dependency management
* **Git** — Version control with clean project structure

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                      Apache Airflow                     │
│                                                         │
│  - DAGs define scheduling, retries, dependencies        │
│  - Spark jobs treated as first-class tasks              │
│                                                         │
└───────────────────────▲─────────────────────────────────┘
                        │
                Triggers Spark Jobs
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                    Apache Spark                         │
│                                                         │
│  - Scala-based Spark applications                       │
│  - Packaged as JARs via SBT                              │
│  - Designed to be stateless and repeatable              │
│                                                         │
└───────────────────────▲─────────────────────────────────┘
                        │
                        ▼
              Logs / Metrics / Data Outputs
```

**Key architectural principles applied:**

* Airflow handles **when & why** a job runs
* Spark handles **how** data is processed
* Jobs are **idempotent and reproducible**
* Build artifacts are explicit and versionable

---

## Key Features

* Modular **Scala Spark jobs** suitable for extension
* Airflow DAGs designed with **clear task boundaries**
* Dockerized local environment mirroring production setups
* Explicit build pipeline using **SBT**
* Clean separation between orchestration, compute, and configuration

---

## Getting Started

### Prerequisites

The project assumes familiarity with standard data engineering tooling:

* Java 8 or 11
* Scala & SBT
* Python 3.8+
* Docker & Docker Compose
* Git

---

### Clone the Repository

```bash
git clone https://github.com/Gmanoori/POC.git
cd POC
```

---

## Build Process (Spark)

Spark jobs are written in Scala and packaged using SBT.

```bash
sbt clean compile
sbt package
```

This produces versioned JAR artifacts under `target/scala-*`, which are then consumed by Spark via Airflow-triggered jobs.

**Why this matters:**

* Mirrors real production Spark deployments
* Makes dependencies explicit
* Enables CI/CD integration

---

## Running the Pipeline Locally

All services can be started using Docker Compose:

```bash
docker-compose up --build
```

Once running:

* **Airflow UI:** [http://localhost:8080](http://localhost:8080)
* DAGs can be triggered manually or scheduled
* Logs are accessible for debugging and observability

This setup ensures **environment parity** and eliminates "works on my machine" issues.

---

## Project Structure (Designed for Scale)

```
POC/
├── dags/                  # Airflow DAG definitions
├── doc/                   # Architectural & design documentation
├── project/               # Internal SBT configuration
├── spark-jars/            # Spark dependency / assembly JARs
├── src/                   # Spark application source code
│   └── main/
│       ├── scala/
│       └── resources/
├── test/                  # Unit & integration tests
├── build.sbt              # Build definition
├── docker-compose.yml     # Runtime orchestration
└── .gitignore
```

This layout mirrors how production data platforms are commonly organized.

---

## Development Philosophy

This project follows several production-grade principles:

* **Idempotent jobs** — safe to rerun
* **Stateless processing** — externalized state
* **Explicit dependencies** — no hidden magic
* **Clear ownership boundaries** — orchestration ≠ compute

---

## Testing

Scala tests can be executed via:

```bash
sbt test
```

The testing structure is intentionally separated to support:

* Unit testing of Spark logic
* Future data quality or integration tests

---

## Extensibility Roadmap

Potential future enhancements:

* Data quality checks (row counts, null thresholds)
* Incremental processing strategies
* CI/CD pipeline integration
* Cloud deployment (EMR / Dataproc / Databricks)
* Metrics & alerting integration

---

## Contributing

Contributions are welcome via standard GitHub workflow:

1. Fork the repository
2. Create a feature branch
3. Commit with clear intent
4. Open a Pull Request

---

## License

No license is currently specified. A license can be added if the project is intended for broader reuse.
