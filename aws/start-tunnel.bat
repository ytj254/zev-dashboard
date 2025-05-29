@echo off
echo Starting SSH tunnel to RDS via EC2...
ssh -i "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\zev-dashboard\aws\zev-key.pem" -L 5433:zev-perf.chw0mom2oauu.us-east-2.rds.amazonaws.com:5432 ubuntu@3.135.230.38
pause
