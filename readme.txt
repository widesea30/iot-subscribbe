sudo apt-get install python3-dev libmysqlclient-dev

sudo python3 -m pip install -r requirements.txt

cd
mkdir logs

sudo crontab -e

@reboot sleep 30; sh /home/ubuntu/iot-subscribe/launcher.sh >/home/ubuntu/logs/cronlog 2>&1