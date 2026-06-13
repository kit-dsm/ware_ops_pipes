#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_DIR}" || exit 1

mkdir -p logs
mkdir -p logs/experiment_runs

MASTER_LOG="logs/run_all_experiments.log"

echo "============================================================" | tee -a "${MASTER_LOG}"
echo "Starting all experiments at $(date)" | tee -a "${MASTER_LOG}"
echo "Project dir: ${PROJECT_DIR}" | tee -a "${MASTER_LOG}"
echo "============================================================" | tee -a "${MASTER_LOG}"

run_experiment() {
    local name="$1"
    shift

    local log_file="logs/experiment_runs/${name}.log"

    echo "" | tee -a "${MASTER_LOG}"
    echo "------------------------------------------------------------" | tee -a "${MASTER_LOG}"
    echo "Starting ${name} at $(date)" | tee -a "${MASTER_LOG}"
    echo "Command: uv run --frozen python $*" | tee -a "${MASTER_LOG}"
    echo "Log file: ${log_file}" | tee -a "${MASTER_LOG}"
    echo "------------------------------------------------------------" | tee -a "${MASTER_LOG}"

    uv run --frozen python "$@" > "${log_file}" 2>&1
    local exit_code=$?

    if [[ ${exit_code} -eq 0 ]]; then
        echo "Finished ${name} successfully at $(date)" | tee -a "${MASTER_LOG}"
    else
        echo "FAILED ${name} with exit code ${exit_code} at $(date)" | tee -a "${MASTER_LOG}"
    fi

    return ${exit_code}
}

FAILED=0

# Foodmart
run_experiment "foodmart" "experiments/run_foodmart.py" || FAILED=1

# Hessler-Irnich / literature instance sets
run_experiment "hessler_irnich_sprp" "experiments/run_hessler_irnich.py" "SPRP" || FAILED=1
run_experiment "hessler_irnich_sprp_ss" "experiments/run_hessler_irnich.py" "SPRP-SS" || FAILED=1
run_experiment "hessler_irnich_bahceci_oencan" "experiments/run_hessler_irnich.py" "BahceciOencan" || FAILED=1
run_experiment "hessler_irnich_muter_oencan" "experiments/run_hessler_irnich.py" "MuterOencan" || FAILED=1
run_experiment "hessler_irnich_henn_waescher_uniform" "experiments/run_hessler_irnich.py" "HennWaescherUniform" || FAILED=1
run_experiment "hessler_irnich_henn_waescher_class_based" "experiments/run_hessler_irnich.py" "HennWaescherClassBased" || FAILED=1

# IBRSP / Kris instance sets
run_experiment "ibrsp_kris_small_corrected" "experiments/run_ibrsp.py" "KrisSmallDataCorrected" || FAILED=1
run_experiment "ibrsp_kris_large" "experiments/run_ibrsp.py" "KrisLargeData" || FAILED=1

echo "" | tee -a "${MASTER_LOG}"
echo "============================================================" | tee -a "${MASTER_LOG}"

if [[ ${FAILED} -eq 0 ]]; then
    echo "All experiments finished successfully at $(date)" | tee -a "${MASTER_LOG}"
    echo "============================================================" | tee -a "${MASTER_LOG}"
    exit 0
else
    echo "Some experiments failed. Check logs/experiment_runs/*.log" | tee -a "${MASTER_LOG}"
    echo "Finished at $(date)" | tee -a "${MASTER_LOG}"
    echo "============================================================" | tee -a "${MASTER_LOG}"
    exit 1
fi