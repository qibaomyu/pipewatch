# pipewatch

A lightweight CLI tool to monitor and alert on data pipeline failures with configurable thresholds.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/youruser/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Monitor a pipeline and alert when the failure rate exceeds a threshold:

```bash
pipewatch monitor --pipeline my_etl_job --threshold 0.05 --interval 60
```

Check the status of all tracked pipelines:

```bash
pipewatch status
```

Configure alert destinations (email, Slack, webhook) via a config file:

```yaml
# pipewatch.yml
alerts:
  slack_webhook: "https://hooks.slack.com/services/..."
  email: "ops-team@example.com"

pipelines:
  my_etl_job:
    threshold: 0.05
    interval: 60
```

Then run:

```bash
pipewatch monitor --config pipewatch.yml
```

---

## Features

- Real-time failure rate monitoring across multiple pipelines
- Configurable alerting thresholds per pipeline
- Supports Slack, email, and custom webhook notifications
- Minimal setup — runs as a single CLI command

---

## License

This project is licensed under the [MIT License](LICENSE).