# lambda_ExecutarBackupIndividual

```
docker build --no-cache -t executarbackupindividual -f Dockerfile.txt .
```

```
aws ecr get-login-password --region sa-east-1 | docker login --username AWS --password-stdin 575108956536.dkr.ecr.sa-east-1.amazonaws.com
```

```
docker tag executarbackupindividual:latest 575108956536.dkr.ecr.sa-east-1.amazonaws.com/executarbackupindividual:latest
```

```
docker push 575108956536.dkr.ecr.sa-east-1.amazonaws.com/executarbackupindividual:latest
```
