# Configuration Guide

The quant backtesting engine uses environment variables and configuration files to manage paths and settings.

## Environment Variables

You can configure the engine using environment variables or a `.env` file in the project root:

```bash
# Optional: Set workspace root to override all relative paths
export WORKSPACE_ROOT=/path/to/your/workspace

# Data paths (relative to workspace root or current directory)
export SYMBOLS_DB_PATH=data/symbols.db
export FX_DB_PATH=data/fx.db
export COST_PROFILES_PATH=quant/data/cost_profiles.yml
export RUNS_DIR=runs
```

## Docker Environment

For Docker environments, you might want to set:

```bash
export WORKSPACE_ROOT=/workspace
export SYMBOLS_DB_PATH=/workspace/data/symbols.db
export FX_DB_PATH=/workspace/data/fx.db
export COST_PROFILES_PATH=/workspace/quant/data/cost_profiles.yml
export RUNS_DIR=/workspace/runs
```

## .env File

Create a `.env` file in the project root:

```env
# Example .env file
WORKSPACE_ROOT=/path/to/your/workspace
SYMBOLS_DB_PATH=data/symbols.db
FX_DB_PATH=data/fx.db
COST_PROFILES_PATH=quant/data/cost_profiles.yml
RUNS_DIR=runs
```

## Default Behavior

If no environment variables are set, the engine uses these relative paths:

- `data/symbols.db` - Symbols database
- `data/fx.db` - FX rates database
- `quant/data/cost_profiles.yml` - Cost profiles configuration
- `runs` - Output directory for backtest results

## Path Resolution

1. If `WORKSPACE_ROOT` is set, all other paths are resolved relative to it
2. Otherwise, paths are resolved relative to the current working directory
3. Absolute paths in environment variables are used as-is
