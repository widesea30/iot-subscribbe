sudo apt-get install python3-dev libmysqlclient-dev

sudo python3 -m pip install -r requirements.txt

- create and edit a custom systemd service to run the shell script at boot by running the following command in the terminal:
sudo nano /etc/systemd/system/mysubscriber.service 

- add content
[Unit]
Description=my subscriber
After=gdm.service

[Service]
Type=oneshot
ExecStart=/home/ubuntu/iot-subscribe/launcher.sh

[Install]
WantedBy=multi-user.target


- start the service by running the following command in the terminal:
sudo systemctl start mysubscriber

- enable the service by running the following command in the terminal:
sudo systemctl enable mysubscriber
