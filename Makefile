setup:
	python -m venv venv
	. venv/Scripts/activate; pip install -r requirements.txt

run:
	. venv/Scripts/activate; python src/app.py

test:
	. venv/Scripts/activate; pytest tests/

notebook:
	. venv/Scripts/activate; jupyter notebook
