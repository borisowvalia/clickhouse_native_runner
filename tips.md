docker build -t ch-coderunner:latest . 
docker buildx build -t cr.yandex/crplf5aivpuaf42fa7a9/ch-coderunner-arm:latest .  
docker run -d --name ch-coderunner -p 8123:8123 -p 9000:9000 ch-coderunner:latest
docker exec -it ch-coderunner clickhouse-client    