sleep 10
airflow upgradedb
sleep 10

sudo airflow scheduler

sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker

airflow scheduler & airflow webserver