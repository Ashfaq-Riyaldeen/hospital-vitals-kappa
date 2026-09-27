# Clinical Scorer Audit Diff: v1 vs v2

**Generated At**: 2026-09-27T07:26:08.151456+00:00  
**Total Patient Evaluations Re-derived**: 17,822  
**Changed Evaluations**: 4,001 (22.45%)  
**Identical Evaluations**: 13,821 (77.55%)  

## 1. Core Kappa Invariant & Control Group Verification

- **COPD Patients (copd_scale2 = True)**: 10 patients  
  - Re-evaluations Changed: 4,001 (upward: 413, of which not explained by oxygen at 93 %+: 0)  
- **Non-COPD Control Group (copd_scale2 = False)**: 30 patients  
  - Re-evaluations Changed: 0  
  - **Bit-Identical Control Proof**: PASSED (Zero difference)  

## 2. Clinical Risk Tier Transition Matrix

| Transition (v1 -> v2) | Patient-Evaluations Count | Clinical Interpretation |
|---|---|---|
| `HIGH -> HIGH` | 325 | unchanged |
| `HIGH -> LOW` | 394 | band changed |
| `HIGH -> LOW_MEDIUM` | 2 | band changed |
| `HIGH -> MEDIUM` | 213 | band changed |
| `LOW -> HIGH` | 4 | band changed |
| `LOW -> LOW` | 15,051 | unchanged |
| `LOW -> MEDIUM` | 149 | band changed |
| `LOW_MEDIUM -> LOW` | 398 | band changed |
| `LOW_MEDIUM -> LOW_MEDIUM` | 88 | unchanged |
| `MEDIUM -> HIGH` | 21 | band changed |
| `MEDIUM -> LOW` | 620 | band changed |
| `MEDIUM -> LOW_MEDIUM` | 19 | band changed |
| `MEDIUM -> MEDIUM` | 538 | unchanged |

## 3. Alarm Impact

- **Scores that drop below NEWS2 5 under v2**: 1035  
- **Scores that rise to NEWS2 5 or more under v2**: 153  

## 4. Patient Detail Summary

| Patient ID | COPD Scale 2 | Evals | Changed | Change % | Mean Delta | Alerts v1 | Alerts v2 |
|---|---|---|---|---|---|---|---|
| `P001` | True | 445 | 330 | 74.16% | +0.52 | 39 | 25 |
| `P002` | True | 444 | 444 | 100.0% | -2.61 | 23 | 1 |
| `P003` | True | 437 | 437 | 100.0% | -2.56 | 25 | 5 |
| `P004` | False | 447 | 0 | 0.0% | +0.00 | 4 | 4 |
| `P005` | False | 447 | 0 | 0.0% | +0.00 | 2 | 2 |
| `P006` | False | 443 | 0 | 0.0% | +0.00 | 7 | 7 |
| `P007` | False | 442 | 0 | 0.0% | +0.00 | 7 | 7 |
| `P008` | True | 441 | 356 | 80.73% | -1.10 | 26 | 20 |
| `P009` | False | 447 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P010` | False | 444 | 0 | 0.0% | +0.00 | 5 | 5 |
| `P011` | False | 449 | 0 | 0.0% | +0.00 | 7 | 7 |
| `P012` | True | 445 | 445 | 100.0% | -2.10 | 36 | 1 |
| `P013` | False | 443 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P014` | False | 445 | 0 | 0.0% | +0.00 | 7 | 5 |
| `P015` | False | 445 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P016` | False | 443 | 0 | 0.0% | +0.00 | 26 | 25 |
| `P017` | False | 445 | 0 | 0.0% | +0.00 | 4 | 4 |
| `P018` | True | 441 | 385 | 87.3% | -1.18 | 17 | 3 |
| `P019` | False | 446 | 0 | 0.0% | +0.00 | 20 | 20 |
| `P020` | False | 439 | 0 | 0.0% | +0.00 | 14 | 14 |
| `P021` | True | 445 | 410 | 92.13% | -1.26 | 4 | 0 |
| `P022` | False | 441 | 0 | 0.0% | +0.00 | 1 | 1 |
| `P023` | False | 449 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P024` | False | 449 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P025` | False | 450 | 0 | 0.0% | +0.00 | 5 | 5 |
| `P026` | False | 440 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P027` | False | 450 | 0 | 0.0% | +0.00 | 13 | 13 |
| `P028` | False | 444 | 0 | 0.0% | +0.00 | 2 | 2 |
| `P029` | False | 449 | 0 | 0.0% | +0.00 | 6 | 6 |
| `P030` | True | 448 | 300 | 66.96% | -0.07 | 25 | 23 |
| `P031` | True | 448 | 448 | 100.0% | -3.00 | 24 | 27 |
| `P032` | False | 449 | 0 | 0.0% | +0.00 | 2 | 2 |
| `P033` | False | 451 | 0 | 0.0% | +0.00 | 21 | 21 |
| `P034` | False | 449 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P035` | False | 449 | 0 | 0.0% | +0.00 | 0 | 0 |
| `P036` | False | 446 | 0 | 0.0% | +0.00 | 6 | 6 |
| `P037` | False | 445 | 0 | 0.0% | +0.00 | 1 | 1 |
| `P038` | False | 450 | 0 | 0.0% | +0.00 | 2 | 2 |
| `P039` | False | 445 | 0 | 0.0% | +0.00 | 4 | 4 |
| `P040` | True | 447 | 446 | 99.78% | -2.21 | 26 | 0 |