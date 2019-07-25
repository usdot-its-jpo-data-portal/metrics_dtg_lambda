Setup Instructions.

Prerequisites:
	- CD to root of repo

- Create a virtualenv at the root of the repo with this command:
virtualenv ./.venv_metrics_lambda_dtg

- Activate that venv with this command:
source ./.venv_metrics_lambda_dtg/bin/activate

- Install all pip dependencies to the package folder with this command:
pip install -r ./requirements.txt -t ./package


Deployment Instructions.
- CD into package folder
- Zip package folder using this command:
zip -r ../lambdapackage.zip ./*

Upload package to lambda using AWS CLI or web console