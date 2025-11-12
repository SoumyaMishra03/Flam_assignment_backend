QueueCTL – Job Queue System with FastAPI Dashboard

Overview
QueueCTL is a production‑grade CLI job queue system with a FastAPI dashboard. It supports job lifecycle management, worker orchestration, DLQ handling, runtime configuration, and real‑time metrics visualization.
--------------------------------------------------------------------------------------------------------------------------------------------------
**Features**
Job Lifecycle States

pending: Waiting to be picked up by a worker

processing: Currently being executed

completed: Successfully executed

failed: Failed, but retryable

dead: Permanently failed (moved to DLQ)

CLI Commands for enqueue, worker management, status, job listing, DLQ operations, and config management

FastAPI Dashboard with charts, metrics, DLQ view, config page, and job detail logs

Graceful shutdown and retry/backoff configuration

Cross‑platform support (tested on PowerShell, cmd.exe, Linux shells)
--------------------------------------------------------------------------------------------------------------------------------------------
**Installation**
git clone <repo-url>
cd queuectl
--------------------------------------------------------------------------------------------------------------------------------------------
**For starting the fast api application**
python -m uvicorn dashboard:app --reload
--------------------------------------------------------------------------------------------------------------------------------------------
**Job Lifecycle Demo**
Enqueue jobs
python cli.py enqueue --id job1 --command "python short.py"
python cli.py enqueue --id job2 --command "python long.py"
python cli.py enqueue --id job_fail --command "badcommand"

Start workers
python cli.py worker

Observe states
python cli.py list --state pending
python cli.py list --state processing
python cli.py list --state completed
python cli.py list --state failed
python cli.py list --state dead
--------------------------------------------------------------------------------------------------------------------------------------------
**FastAPI Dashboard**
Home: Recent jobs table, pie chart of states, line chart of average duration trend

Metrics: Job counts, average duration cards

DLQ: Dead jobs with error details and retry option

Config: Runtime configuration values

Job Detail: Lifecycle info, stdout/stderr, log download
--------------------------------------------------------------------------------------------------------------------------------------------
Video 1 and 2 have been added for better understanding of the application
