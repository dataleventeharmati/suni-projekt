```mermaid
flowchart LR
    A[Excel Source] --> B[Bronze Ingest]
    B --> C[Silver Clean & Validate]
    C --> D[Data Profiling]
    D --> E[Data Quality Gate]
    E --> F[Gold KPI Mart]
    F --> G[Reports]
    G --> H[KPI CSV]
    G --> I[HTML Report]
    G --> J[Data Quality JSON]
    F --> K[Delivery Package]
    G --> L[Versioned Runs]
