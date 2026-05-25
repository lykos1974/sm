# BTC Followthrough Validation (Data-contract fix rerun)

## Regime distribution
- FAST_MEAN_REVERSION: 2
- VOLATILE_CHOP: 1
- TREND_CONTINUATION: 1

## Example rows by regime
### TREND_CONTINUATION
- symbol= ts= pattern=LOW_POLE traj=0.0|0.411765|0.0|0.647059|0.0|0.882353|0.0|1.0 comp=3.0 fav=50.0 adv=0.0
### VOLATILE_CHOP
- symbol= ts= pattern=LOW_POLE traj=-1.0|0.0|-0.333333|0.166667|-0.083333|0.5|0.0|0.833333|0.0|1.0 comp=0.5625 fav=30.0 adv=17.0
### SIDEWAYS_COMPRESSION
- none
### FAST_MEAN_REVERSION
- symbol= ts= pattern=HIGH_POLE traj=-0.454545|0.0|-0.818182|0.0|-1.0 comp=1.178571 fav=0.0 adv=25.0
- symbol= ts= pattern=HIGH_POLE traj=-1.0 comp=1.0 fav=0.0 adv=3.0
### FAILED_REVERSAL
- none

## Trajectory value ranges
- min=-1.000000
- max=1.000000
- unique_count=13

## Compression distribution
- min=0.562500
- max=3.000000
- mean=1.435268
- q25=0.890625
- median=1.089285
- q75=1.633928

## Top continuation buckets
- pole=<=8 retrace=<0.75 enhanced=False: 2
- pole=9-13 retrace=<0.75 enhanced=True: 1
- pole=14-20 retrace=<0.75 enhanced=False: 1

## Row counts
- pnf_columns_rows=14
- pole_patterns_rows=4
- labeled_rows=4
- followthrough_rows=4
