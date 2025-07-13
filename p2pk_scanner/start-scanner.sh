#!/bin/bash

#source ../venv/bin/activate && python hydra_mode_scanner.py --threads 32 --target-depth 16 --worker-profile --profile --profile-output logs/profile_results.txt
source ../venv/bin/activate && python hydra_mode_scanner.py --batch-rpc --rpc-batch-size 50 --threads 64 --target-depth 8 --quick-scan
