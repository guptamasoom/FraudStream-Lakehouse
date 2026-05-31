# Fraud Rules

The project uses explainable rules rather than a black-box model so students can understand why a transaction is risky.

## Current Rules

| Rule | Meaning |
| --- | --- |
| `HIGH_AMOUNT` | Transaction amount is unusually high |
| `COUNTRY_MISMATCH` | IP, billing, or shipping country disagree |
| `NEW_DEVICE_HIGH_AMOUNT` | High amount from a new device |
| `HIGH_VELOCITY` | Customer has many transactions in the batch |
| `MANY_CUSTOMERS_ON_DEVICE` | Device appears across multiple customers |
| `FAILED_LOGIN_BEFORE_PURCHASE` | Customer had repeated failed login events |
| `CARD_NOT_PRESENT_RISK` | Card-not-present payment with geography mismatch |

## Risk Bands

| Band | Score |
| --- | ---: |
| LOW | 0-39 |
| MEDIUM | 40-69 |
| HIGH | 70-100 |

## Why Rule-Based First?

Rule-based scoring is practical for a portfolio project because it is:

- explainable
- testable
- auditable
- easy to extend
- useful as a baseline before machine learning

The next upgrade would be a feature store and supervised fraud model using chargebacks as delayed labels.
