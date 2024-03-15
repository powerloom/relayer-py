docker build -t powerloom-relayer .

if ! [ -x "$(command -v docker-compose)" ]; then
    echo 'docker compose not found, trying to see if compose exists within docker';
    docker compose up -V --abort-on-container-exit
else
    docker compose up -V --abort-on-container-exit
fi
