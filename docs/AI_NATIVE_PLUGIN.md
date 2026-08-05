# AI-Native Plugin Guide

## Install from PyPI

```bash
pip install PermutiveAPI
```

## Install from Git

```bash
pip install git+https://github.com/fatmambot33/PermutiveAPI.git
```

For local development:

```bash
git clone https://github.com/fatmambot33/PermutiveAPI.git
cd PermutiveAPI
pip install -e '.[dev]'
```

## Install the Codex plugin

```bash
codex plugin marketplace add fatmambot33/PermutiveAPI --ref main
codex plugin add permutiveapi@fatmambot33-permutiveapi
```

The plugin exposes a typed, versioned contract with deterministic entry-point discovery and capability metadata. Before network operations, configure local credentials and validate them:

```bash
permutiveapi configure
permutiveapi doctor
```

Credentials are loaded only from the local `.env`; secret values are never printed, committed, or remotely stored.
