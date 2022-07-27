i#!/bin/sh

# First argument is the folder containing the python code (default to current folder).
python_folder="."
if [ "$1" != "" ]; then
  python_folder="$1"
fi

if [ -f "${python_folder}/requirements.txt" ]; then
  pip3 install -q -r ${python_folder}/requirements.txt
fi

echo -e "\nPython files:"
ls ${python_folder}/*.py

# Run python linting. Ignoring E501 (line too long)
echo -e "\nLinting python files..."
flake8 --ignore=E1,E23,W503,E226,W391 ${python_folder}
resp=$?
# Exit if there are any errors 
if [ ${resp} -ne 0 ]; then
  exit 1
fi

# Execute unit tests
echo -e "\nExecuting unit tests..."
found_errors=0
for python_file in ${python_folder}/*.py; do
  echo "File Name "  ${python_file}
  python  ${python_file}
  if [ $? -ne 0 ]; then
    found_errors=1
  fi
done
# Exit if there are any errors 
if [ ${found_errors} -ne 0 ]; then
  exit 1
fi
