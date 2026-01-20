SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${PROJECT_DIR}/.venv/bin/activate"

cd "${PROJECT_DIR}" || exit 1

# Activate the virtual environment
source "${VENV_PATH}"

# Set up Python path to include src directory
export PYTHONPATH="${PYTHONPATH}:${PROJECT_DIR}/src"

# Change to the experiments/ directory for output
cd "${PROJECT_DIR}/experiments" || exit 1

# Run the pipeline script in background with logging
nohup python run_iopvrp.py > pipeline_run.txt &
echo $! > process.pid

echo "Process started. PID saved in process.pid"