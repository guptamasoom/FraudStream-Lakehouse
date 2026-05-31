# Data Contracts

Data contracts make the project explainable and production-minded. Each source stream has an explicit schema in `data_contracts/`.

## Streams

| Stream | Topic | Contract |
|---|---|---|
| Transactions | `fraud_transactions` | `data_contracts/transactions_schema.json` |
| Logins | `fraud_logins` | `data_contracts/logins_schema.json` |
| Devices | `fraud_devices` | `data_contracts/devices_schema.json` |
| Chargebacks | `fraud_chargebacks` | `data_contracts/chargebacks_schema.json` |

## Contract Principles

- Every event must have an `event_id`.
- Every event must carry an `event_timestamp`.
- Customer, device, merchant, and transaction IDs are preserved when available.
- Raw events are retained in Bronze before any transformation.
- Bad records are quarantined with failure reasons.

## Why Contracts Matter

Contracts prevent silent schema drift. In a real fraud platform, a missing timestamp, renamed customer field, or negative amount can break fraud monitoring, dashboards, and downstream investigations. FraudStream catches these issues in Silver and keeps evidence in quarantine.

