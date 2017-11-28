if [ -s /etc/redhat-release ]; then
	sudo yum install maven -y
elif [ -f /etc/debian_version ]; then
	sudo apt install maven -y
fi

