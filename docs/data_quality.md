# Data Quality Framework

Fraud pipelines must be strict because poor data can either miss fraud or create false alarms.

## Quality Checks

- `event_id_present`
- `event_timestamp_present`
- `customer_id_present`
- `transaction_id_present`
- `amount_non_negative`
- `valid_ip_country`
- `valid_billing_country`
- `chargeback_amount_non_negative`

## Outputs

The quality framework produces two outputs:

1. **Scorecard:** table-level pass/fail metrics.
2. **Quarantine:** failed records with `check_name`, `reason_code`, and `severity`.

This design keeps bad data visible without allowing it to pollute trusted Silver and Gold tables.
