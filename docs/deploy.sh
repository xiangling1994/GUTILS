#!/bin/bash
set -ev

cd docs
conda install --file requirements.txt
sphinx-apidoc -M -f -o api ../gutils ../gutils/tests
make html
doctr deploy --built-docs=_site/html .
cd ..
