#!/bin/bash

# Set environment variables
export CL_LAB="your-guid"
export CL_POOL="your-pool"  
export CL_CONF="/path/to/ceph.conf"
export CL_KEYRING="/path/to/keyring"
export DRY_RUN="true" # if true debug is always true
export DEBUG="true"

# --- Verification Checks ---
echo "Verifying environment variables..."

all_vars_ok=true
# List all the variables you want to check in this string
vars_to_check="CL_LAB CL_POOL CL_CONF CL_KEYRING DRY_RUN DEBUG"

for var in $vars_to_check; do
  # The construct ${!var} gets the value of the variable whose name is stored in 'var'
  if [ -z "${!var}" ]; then
    echo "  [x] ERROR: $var is not set."
    all_vars_ok=false
  else
    echo "  [v] OK: $var is set to '${!var}'"
  fi
done

echo "-------------------------"
if [ "$all_vars_ok" = true ]; then
  echo "All checks passed. Ready to run script."
else
  echo "One or more checks failed. Please set all required variables."
fi