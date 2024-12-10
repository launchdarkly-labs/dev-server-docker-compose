# Launchdarkly CLI dev-server With Docker Compose Demo

```bash
LD_ACCESS_TOKEN=replace-me docker compose up
```

The Launchdarkly CLI dev-server has a feature to [start and sync](https://docs.launchdarkly.com/guides/flags/ldcli-dev-server#starting-and-syncing)

This repo is a demo of how to use dev-server start and sync with [docker-compose](https://docs.docker.com/compose/) to enable a consistent setup for local development 


## Key Points:
1. Configure [ldcli container](docker-compose.yml)
2. Customize [variables for dev-server sync](.env)
3. [configure LDClient](app/main.go) to accept the dev-server uri
2. Specify [which flag to evaluate for the demo app](.env)

## Running the Code

```bash
LD_ACCESS_TOKEN=replace-me docker compose up
```
