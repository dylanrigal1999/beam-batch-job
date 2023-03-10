echo Create virtual environment
python3 -m venv .venv
echo Activate virtual environment
. .venv/bin/activate
echo Install requirements
pip install -r requirements.txt
echo Run unit test
python3 -m unittest
echo Run pipeline
python3 -m pipeline
echo Pieline done. Display output:
cat output/results.csv-00000-of-00001
echo run done.
