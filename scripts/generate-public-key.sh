if [ ! -f ~/.ssh/id_rsa.pub ]; then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
else
    echo "~/.ssh/id_rsa already exists."
fi
