SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VENV_DIR=$SCRIPT_DIR/venv

if ! [ -d $VENV_DIR ]; then
    python3.12 -m venv $VENV_DIR
fi

source $VENV_DIR/bin/activate
pip install -r $SCRIPT_DIR/requirements.txt