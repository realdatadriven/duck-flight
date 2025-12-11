# Duck-Flight

**Duck-Flight** is an experimental Arrow Flight SQL server designed to be fully compatible with the DuckDB `airport` extension while leveraging DuckDB's native Arrow support. It is built on top of the excellent [`airport-go`](https://github.com/hugr-lab/airport-go) implementation.

The goal of Duck-Flight is to provide a lightweight, high‑performance, multi-database Arrow Flight server with automatic schema discovery and seamless integration with DuckDB as a query engine.

---

## ✈️ Vision

Duck-Flight aims to:

* Serve data via **Arrow Flight SQL**, fully compatible with DuckDB's `airport` client.
* Use DuckDB as the primary execution engine.
* Allow **attaching multiple DuckDB / SQLite / Parquet / CSV sources**, where each attachment becomes a *schema* in the Airport catalog.
* Automatically discover tables inside each attached database or directory.
* Translate **DuckDB table schema → Arrow schema** automatically.
* Proxy SQL queries through DuckDB and return Arrow RecordBatches.

All of the above features form the long‑term vision of the project. **Most of these steps are still TODO**, but the foundation is set.

---

## 🧩 Current Status

This repository currently includes:

* A basic Arrow Flight SQL server built on `airport-go`.
* Initial DuckDB integration.
* YAML-based configuration loader.
* WIP utilities for DuckDB → Arrow type translation (`duckTypeToArrow`).

The parts listed above are functional foundations and will expand as the project evolves.

---

## 📦 Configuration Example

Duck-Flight uses a YAML configuration file to define the databases that will be attached.

Example: `config.yaml`

```yaml
description: |
  This YAML describes attachable database schemas for an Arrow Flight
  server backed by DuckDB. Each schema block includes attach SQL, detach SQL,
  and optional setup commands (INSTALL/LOAD) depending on the backend engine.

schemas:
  - name: schema_name
    description: schema_description
    version: 1
    before_sql: |
      INSTALL SQLITE;
      LOAD SQLITE;
    main_sql: |
      ATTACH 'file_path' AS schema_name (TYPE SQLITE);
    after_sql: |
      USE memory;
      DETACH schema_name;

  - name: schema_name2
    description: schema_description2
    version: 1
    before_sql: |
      INSTALL POSTGRES;
      LOAD POSTGRES;
    main_sql: |
      ATTACH 'pg_conn_string' AS schema_name2 (TYPE POSTGRES);
    after_sql: |
      USE memory;
      DETACH schema_name2;
```

This example assumes you have a **TPC-H SF=1** database exported to SQLite located at `./database/tpch.db`.

When Duck-Flight starts:

* This database will be attached as the schema `tpch`.
* All its tables will be discovered.
* Their DuckDB types will be mapped to Arrow types.
* They will be exposed in the Airport SQL catalog.

---

## 🚀 Basic Example

### 1. Install dependencies

You need DuckDB installed with Arrow support and Go ≥1.25.

### 2. Create a TPC-H SQLite DB (example)

```bash
duckdb tpch.duckdb -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=1); COPY (SELECT * FROM orders) TO 'database/tpch.db' (FORMAT SQLITE);"
```

(Alternatively, use any existing SQLite database.)

### 3. Run Duck-Flight

```bash
go run ./cmd/duckflight --config config.yaml
```

### 4. Connect from DuckDB

```sql
INSTALL airport;
LOAD airport;

CREATE SECRET (TYPE airport, HOST '127.0.0.1', PORT 8088);

SELECT * FROM tpch.orders LIMIT 10;
```

You should receive Arrow batches streamed from Duck-Flight.

---

## 🛠️ Roadmap / TODO

### ✔️ Completed

* Initial `airport-go` integration
* Basic Flight SQL server
* YAML configuration support

### 🔧 In Progress

* DuckDB → Arrow type translation (`duckTypeToArrow`)
* Table discovery for attached DBs

### 🔮 Planned

* Attach DuckDB, SQLite, Parquet, CSV, and folder-based datasets
* Multi-schema Airport catalog
* Airport authentication support
* Fully automatic schema + metadata propagation
* Pushdown optimization using DuckDB

---

## 🤝 Contributing

Contributions, ideas, and feedback are welcome! This is an early-phase experimental project.

---

## 📜 License

MIT License.

---

## 💡 Summary

Duck-Flight is a new idea inspired by the power of DuckDB and Arrow Flight. The mission is to create a flexible, fast, extensible data access layer that exposes multiple datasets over Arrow Flight with intelligent schema handling and DuckDB-powered SQL execution.

This is the very beginning—but the trajectory is exciting. 🚀
