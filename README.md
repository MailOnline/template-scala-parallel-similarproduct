# Pythia engine

Based on http://docs.prediction.io/templates/similarproduct/

Compared to the upstream «similar product» engine template from
PredictionIO, our version supports some extra data source settings in
`engine.json`:

To filter events by time:

- `startTime` (in ISO8601; required)
- `untilTime` (in ISO8601; optional, defaults to current time)

We use JdbcRDD directly (all these are required):

- `jdbcUrl`
- `jdbcUser`
- `jdbcPass`
- `jdbcTable`

All settings are populated at training time via
https://github.com/MailOnline/pythia/blob/master/playbooks/train.yml
