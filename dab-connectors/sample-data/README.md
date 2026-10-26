Synthetic data made up for the POC. No real people, customers or orders.

Each file contains a few deliberately bad rows so the quarantine path is exercised:

| file | rows | bad rows |
|---|---|---|
| `orders/orders_2026-01.csv` | 10 data rows + 1 comment line | missing `customer_id` (1005), impossible date (1006), non-numeric amount (1007), missing field (1008); csv v1 also quarantines the comment line |
| `customers/customers_2026-01.psv` | 8 | impossible date (C005), missing `customer_id`, extra field (C007) |
