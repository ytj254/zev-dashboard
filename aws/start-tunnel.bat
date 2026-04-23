@echo off
echo Starting SSH tunnel to RDS via EC2...
ssh -i "%~dp0zev-key.pem" -L 5433:zev-perf.chw0mom2oauu.us-east-2.rds.amazonaws.com:5432 ubuntu@3.140.99.50
pause


